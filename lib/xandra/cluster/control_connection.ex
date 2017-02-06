defmodule Xandra.Cluster.ControlConnection do
  use Connection

  alias Xandra.{Frame, Protocol, Connection.Utils}

  require Logger

  @default_timeout 5_000
  @socket_options [packet: :raw, mode: :binary, active: false]

  defstruct [:address, :port, :cluster, :socket, new: true, buffer: <<>>]

  def start_link(cluster, address, port) do
    state = %__MODULE__{cluster: cluster, address: address, port: port}
    Connection.start_link(__MODULE__, state)
  end

  def init(state) do
    {:connect, :init, state}
  end

  def connect(_action, %__MODULE__{address: address, port: port} = state) do
    case :gen_tcp.connect(address, port, @socket_options, @default_timeout) do
      {:ok, socket} ->
        send(self(), :activate)
        {:ok, %{state | socket: socket}}
      {:error, _reason} ->
        {:backoff, 5_000, state}
    end
  end

  def handle_info(:activate, %__MODULE__{socket: socket} = state) do
    with {:ok, supported_options} <- Utils.request_options(socket),
         :ok <- startup_connection(socket, supported_options),
         :ok <- register_to_events(socket),
         :ok <- :inet.setopts(socket, active: :true),
         {:ok, state} <- report_active(state) do
      {:noreply, state}
    else
      {:error, reason} -> {:disconnect, {:error, reason}, state}
    end
  end

  def handle_info({:tcp_error, socket, reason}, %__MODULE__{socket: socket} = state) do
    {:disconnect, {:error, reason}, state}
  end

  def handle_info({:tcp_closed, socket}, %__MODULE__{socket: socket} = state) do
    {:disconnect, {:error, :closed}, state}
  end

  def handle_info({:tcp, socket, data}, %__MODULE__{socket: socket} = state) do
    state = %{state | buffer: state.buffer <> data}
    {:noreply, report_event(state)}
  end

  def disconnect({:error, _reason}, %__MODULE__{} = state) do
    :gen_tcp.close(state.socket)
    {:connect, :reconnect, %{state | socket: nil, buffer: <<>>}}
  end

  defp report_active(%{new: false} = state) do
    {:ok, state}
  end

  defp report_active(%{new: true, cluster: cluster, socket: socket} = state) do
    with {:ok, {address, port}} <- :inet.peername(socket) do
      Xandra.Cluster.start_pool(cluster, address, port)
      {:ok, %{state | new: false, address: address}}
    end
  end

  defp startup_connection(socket, supported_options) do
    %{"CQL_VERSION" => [cql_version | _]} = supported_options
    requested_options = %{"CQL_VERSION" => cql_version}
    Utils.startup_connection(socket, requested_options)
  end

  defp register_to_events(socket) do
    payload =
      Frame.new(:register)
      |> Protocol.encode_request(["STATUS_CHANGE"])
      |> Frame.encode()

    with :ok <- :gen_tcp.send(socket, payload),
         {:ok, %Frame{} = frame} <- Utils.recv_frame(socket) do
      :ok = Protocol.decode_response(frame)
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp report_event(%{cluster: cluster, buffer: buffer} = state) do
    case decode_frame(buffer) do
      {frame, rest} ->
        status_change = Protocol.decode_response(frame)
        Logger.debug("Received STATUS_CHANGE event: #{inspect(status_change)}")
        Xandra.Cluster.update(cluster, status_change)
        %{state | buffer: rest}
      :error ->
        state
    end
  end

  defp decode_frame(buffer) do
    header_length = Frame.header_length()
    case buffer do
      <<header::size(header_length)-bytes, rest::binary>> ->
        body_length = Frame.body_length(header)
        case rest do
          <<body::size(body_length)-bytes, rest::binary>> ->
            {Frame.decode(header, body), rest}
          _ ->
            :error
        end
      _ ->
        :error
    end
  end
end
