defmodule Xandra.Cluster do
  @behaviour DBConnection.Pool

  alias __MODULE__.{ControlConnection, StatusChange}

  defstruct [:options, active: %{}, idle: %{}]

  def ensure_all_started(_opts, _type) do
    {:ok, []}
  end

  def child_spec(module, options, child_options) do
    Supervisor.Spec.worker(__MODULE__, [module, options], child_options)
  end

  def start_link(Xandra.Connection, options) do
    # TODO: Remove.
    options = Keyword.put(options, :nodes, [{'127.0.0.1', 9042}, {'127.0.0.2', 9042}, {'127.0.0.3', 9042}])

    {name, options} = Keyword.pop(options, :name)
    GenServer.start_link(__MODULE__, options, name: name)
  end

  def init(options) do
    {nodes, options} = Keyword.pop(options, :nodes)
    start_controll_connections(nodes)
    {:ok, %__MODULE__{options: options}}
  end

  defp start_controll_connections(nodes) do
    cluster = self()
    for {host, port} <- nodes do
      ControlConnection.start_link(cluster, host, port)
    end
  end

  def checkout(cluster, options) do
    GenServer.call(cluster, {:checkout, options}) |> IO.inspect
  end

  def checkin(pool_ref, conn_state, options) do
    DBConnection.Connection.checkin(pool_ref, conn_state, options)
  end

  def start_pool(cluster, address, port) do
    GenServer.cast(cluster, {:start_pool, address, port})
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
    # pool = Map.fetch!(state.active, {127, 0, 0, 2})
    {:reply, DBConnection.Connection.checkout(pool, options), state}
  end

  def handle_cast({:start_pool, address, port}, %__MODULE__{} = state) do
    %{options: options, active: active} = state
    options = [host: address, port: port] ++ options
    {:ok, pool} = DBConnection.Connection.start_link(Xandra.Connection, options)
    {:noreply, %{state | active: Map.put(active, address, pool)}}
  end

  def handle_cast({:update, %StatusChange{} = status_change}, %__MODULE__{} = state) do
    {:noreply, toggle_pool(state, status_change)} |> IO.inspect
  end

  defp toggle_pool(state, %{effect: "UP", address: address}) do
    {idle, active} = move_pool(state.idle, state.active, address)
    %{state | active: active, idle: idle}
  end

  defp toggle_pool(state, %{effect: "DOWN", address: address}) do
    {active, idle} = move_pool(state.active, state.idle, address)
    %{state | active: active, idle: idle}
  end

  defp move_pool(source, target, address) do
    case Map.pop(source, address) do
      {nil, _source} ->
        {source, target}
      {pool, source} ->
        {source, Map.put(target, address, pool)}
    end
  end
end
