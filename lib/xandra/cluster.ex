defmodule Xandra.Cluster do
  @behaviour DBConnection.Pool

  alias __MODULE__.ControlConnection

  defstruct [active: %{}, idle: %{}]

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
    active = start_connections(module, nodes, options)
    {:ok, %__MODULE__{active: active}}
  end

  defp start_connections(module, nodes, options) do
    # TODO: use "host:port".
    for {host, port} <- nodes, into: %{} do
      options = [host: host, port: port] ++ options
      options = Keyword.put(options, :prepared_cache, Xandra.Prepared.Cache.new)
      {:ok, pid} = DBConnection.Connection.start_link(module, options)
      {:ok, address} = :inet.parse_address(host)
      {address, pid}
    end
  end

  def checkout(cluster, options) do
    GenServer.call(cluster, {:checkout, options})
  end

  def checkin(pool_ref, conn_state, options) do
    DBConnection.Connection.checkin(pool_ref, conn_state, options)
  end

  def update(cluster, status_change) do
    GenServer.cast(cluster, {:update, status_change})
  end

  def disconnect(pool_ref, error, conn_state, options) do
    DBConnection.Connection.disconnect(pool_ref, error, conn_state, options)
  end

  def stop(pool_ref, error, conn_state, options) do
    DBConnection.Connection.stop(pool_ref, error, conn_state, options)
  end

  def handle_call({:checkout, options}, _from, %__MODULE__{} = state) do
    {_address, pool} = Enum.random(state.active)
    {:reply, DBConnection.Connection.checkout(pool, options), state}
  end

  def handle_cast({:update, {"UP", {address, _port}}}, %__MODULE__{} = state) do
    state =
      case Map.pop(state.idle, address) do
        {nil, _idle} ->
          state
        {connection, idle} ->
          active = Map.put(state.active, address, connection)
          %{state | active: active, idle: idle}
      end
    IO.inspect state
    {:noreply, state}
  end

  def handle_cast({:update, {"DOWN", {address, _port}}}, %__MODULE__{} = state) do
    state =
      case Map.pop(state.active, address) do
        {nil, _active} ->
          state
        {connection, active} ->
          idle = Map.put(state.idle, address, connection)
          %{state | active: active, idle: idle}
      end
    IO.inspect state
    {:noreply, state}
  end

  # defp move_connection() do
  # end
end
