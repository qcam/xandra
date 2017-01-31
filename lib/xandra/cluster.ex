defmodule Xandra.Cluster do
  @behaviour DBConnection.Pool

  alias __MODULE__.ControlConnection

  defstruct [pools: []]

  def ensure_all_started(_opts, _type) do
    {:ok, []}
  end

  def child_spec(module, options, child_options) do
    Supervisor.Spec.worker(__MODULE__, [module, options], child_options)
  end

  def start_link(module, options) do
    {name, options} = Keyword.pop(options, :name)
    GenServer.start_link(__MODULE__, {module, options}, name: name)
  end

  def init({module, options}) do
    {nodes, options} = Keyword.pop(options, :nodes)
    [{host, port} | _] = nodes = [{'127.0.0.1', 9042}, {'127.0.0.2', 9042}, {'127.0.0.3', 9042}]
    {:ok, _pid} = ControlConnection.start_link(self(), host, port)
    pools = start_pools(module, nodes, options)
    {:ok, %__MODULE__{pools: pools}}
  end

  defp start_pools(module, nodes, options) do
    # TODO: use "host:port".
    for {host, port} <- nodes do
      options = [host: host, port: port] ++ options
      {:ok, pid} = DBConnection.Poolboy.start_link(module, options)
      pid
    end
  end

  def checkout(cluster, options) do
    GenServer.call(cluster, {:checkout, options})
  end

  def checkin(pool_ref, conn_state, options) do
    DBConnection.Poolboy.checkin(pool_ref, conn_state, options)
  end

  def update(cluster, status_change) do
    GenServer.cast(cluster, {:update, status_change})
  end

  def disconnect(pool_ref, error, conn_state, options) do
    DBConnection.Poolboy.disconnect(pool_ref, error, conn_state, options)
  end

  def stop(pool_ref, error, conn_state, options) do
    DBConnection.Poolboy.stop(pool_ref, error, conn_state, options)
  end

  def handle_call({:checkout, options}, _from, %__MODULE__{} = state) do
    pool = Enum.random(state.pools)
    {:reply, DBConnection.Poolboy.checkout(pool, options), state}
  end

  def handle_cast({:update, {"UP", {address, _port}}}, %__MODULE__{} = state) do
    {:noreply, state}
  end

  def handle_cast({:update, {"DOWN", {address, _port}}}, %__MODULE__{} = state) do
    {:noreply, state}
  end
end
