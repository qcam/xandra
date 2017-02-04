defmodule Xandra.Cluster.ControlConnection do
  use Connection

  alias Xandra.{Frame, Protocol, Connection.Utils}

  require Logger

  @default_timeout 5_000
  @socket_options [packet: :raw, mode: :binary, active: false]

  defstruct [:socket, :cluster, buffer: <<>>]

  def start_link(cluster, host, port) do
    Connection.start_link(__MODULE__, {cluster, host, port})
  end

  def init(params) do
    {:connect, :init, params}
  end

  def connect(:init, {cluster, host, port}) do
    with {:ok, socket} <- connect(host, port, @socket_options, @default_timeout),
         {:ok, supported_options} <- Utils.request_options(socket),
         :ok <- startup_connection(socket, supported_options),
         :ok <- register_to_events(socket),
         :ok <- :inet.setopts(socket, active: :true) do
      {:ok, %__MODULE__{socket: socket, cluster: cluster}}
    else
      {:error, reason} ->
        {:stop, :error, reason}
    end
  end

  def handle_info({:tcp, socket, data}, %{socket: socket} = state) do
    {:noreply, %{state | buffer: maybe_report_event(state.cluster, state.buffer <> data)}}
  end

  defp connect(host, port, options, timeout) do
    with {:error, reason} <- :gen_tcp.connect(host, port, options, timeout),
         do: {:error, reason}
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

  defp maybe_report_event(cluster, buffer) do
    case decode_frame(buffer) do
      {frame, rest} ->
        status_change = Protocol.decode_response(frame)
        Logger.debug("Received STATUS_CHANGE event: #{inspect(status_change)}")
        Xandra.Cluster.update(cluster, status_change)
        rest
      :error ->
        buffer
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
