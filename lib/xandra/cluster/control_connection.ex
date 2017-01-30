defmodule Xandra.Cluster.ControlConnection do
  use Connection

  alias Xandra.{Frame, Protocol, Connection.Utils}

  require Logger

  @default_timeout 5_000
  @socket_options [packet: :raw, mode: :binary, active: false]

  defstruct [:socket]

  def start_link(host, port) do
    Connection.start_link(__MODULE__, {host, port})
  end

  def init({_host, _port} = params) do
    {:connect, :init, params}
  end

  def connect(:init, {host, port}) do
    with {:ok, socket} <- connect(host, port, @socket_options, @default_timeout) |> IO.inspect,
         {:ok, supported_options} <- Utils.request_options(socket),
         :ok <- startup_connection(socket, supported_options),
         :ok <- register_to_events(socket),
         :ok <- :inet.setopts(socket, active: :once) do
      {:ok, %__MODULE__{socket: socket}}
    else
      {:error, reason} ->
        {:stop, :error, reason}
    end
  end

  def handle_info({:tcp, socket, data}, %{socket: socket} = state) do
    with {:ok, frame} <- decode_frame(data),
         # %SchemaChange{} = schema_change = Protocol.decode_response(frame),
         schema_change = Protocol.decode_response(frame),
         Logger.debug("Received SCHEMA_CHANGE event: #{inspect(schema_change)}"),
         :ok <- :inet.setopts(socket, active: :once) do
      {:noreply, state}
    end
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

  defp decode_frame(data) do
    header_length = Frame.header_length()
    <<header::size(header_length)-bytes, rest::binary>> = data
    body_length = Frame.body_length(header)
    <<body::size(body_length)-bytes>> = rest
    {:ok, Frame.decode(header, body)}
  end
end
