defmodule Xandra.EventServer do
  @moduledoc false

  use Connection

  alias Xandra.{Frame, Protocol, Rows, SchemaChange, TypeParser}

  require Logger

  @default_timeout 5_000
  @default_sock_opts [packet: :raw, mode: :binary, active: false]

  defstruct [
    :sock,
    :db_connection,
    :column_specs,
    :host,
    :port,
  ]

  ## Public API

  def start_link(host, port) do
    Connection.start_link(__MODULE__, {host, port})
  end

  # TODO: supervisor_spec here

  ## Connection callbacks

  def init({_host, _port} = connection_params) do
    {:connect, :init, connection_params}
  end

  def connect(:init, {host, port}) do
    case :gen_tcp.connect(host, port, @default_sock_opts, @default_timeout) do
      {:ok, sock} ->
        {:ok, options} = request_options(sock)
        startup_connection(sock, options)
        :ok = register_to_events(sock)

        with {:ok, db_connection} <- Xandra.start_link(host: host, port: port, event_server: false),
             {:ok, column_specs} <- fetch_all_column_specs(db_connection) do
          :ok = :inet.setopts(sock, active: :once)
          state = %__MODULE__{
            sock: sock,
            db_connection: db_connection,
            column_specs: column_specs,
          }
          Logger.debug("Event server started (options: #{inspect(options)})")
          {:ok, state}
        else
          {:error, %Xandra.Connection.Error{} = error} ->
            Logger.error("Failed to start event server: #{Exception.message(error)}")
            {:stop, :error, %__MODULE__{}}
          {:error, reason} ->
            Logger.error("Failed to start event server: #{inspect(reason)}")
            {:stop, :error, %__MODULE__{}}
        end
      {:error, reason} ->
        {:stop, reason, %__MODULE__{}}
    end
  end

  def handle_info({:tcp, sock, data}, %{sock: sock} = state) do
    with {:ok, frame} <- next_frame(data) do
      %SchemaChange{} = schema_change = Protocol.decode_response(frame)
      Logger.debug("Received SCHEMA_CHANGE event: #{inspect(schema_change)}")
      with {:ok, state} <- handle_schema_change(state, schema_change),
           :ok = :inet.setopts(sock, active: :once),
           do: {:noreply, state}
    end
  end

  # CREATED:
  #   * the table is created
  #
  # UPDATED:
  #   * the type of a column in the table is changed
  #   * a column in a table is renamed
  #   * a column is added to or dropped from a table
  #
  # In all those cases for simplicity we can just remove all column specs
  # related to the subject table from the state and replace them with the new
  # column specs obtained by doing a new prepared query.
  defp handle_schema_change(state, %SchemaChange{target: "TABLE", effect: effect} = schema_change)
      when effect in ["CREATED", "UPDATED"] do
    %{keyspace: keyspace, subject: table} = schema_change.options

    {:ok, column_specs} = fetch_column_specs_for_table(state.db_connection, keyspace, table)
    new_state = replace_column_specs_for_table(state, keyspace, table, column_specs)

    Logger.debug("Updated specs, all the specs now are: #{inspect(new_state.column_specs)}")

    {:ok, new_state}
  end

  # When a table is dropped, we can just remove all column specs for that table
  # from the state.
  defp handle_schema_change(state, %SchemaChange{target: "TABLE", effect: "DROPPED"} = schema_change) do
    %{keyspace: dropped_keyspace, subject: dropped_table} = schema_change.options
    new_state = replace_column_specs_for_table(state, dropped_keyspace, dropped_table, [])
    {:ok, new_state}
  end

  defp handle_schema_change(state, %SchemaChange{target: "KEYSPACE", effect: effect} = schema_change)
      when effect in ["CREATED", "UPDATED", "DROPPED"] do
    Logger.debug("Keyspace \"#{schema_change.options.keyspace}\" was #{effect}, ignoring event")
    {:ok, state}
  end

  defp replace_column_specs_for_table(state, keyspace_to_replace, table_to_replace, column_specs) do
    new_column_specs =
      state.column_specs
      |> Enum.reject(&match?({^keyspace_to_replace, ^table_to_replace, _column, _type}, &1))
      |> Enum.into(column_specs)

    %{state | column_specs: new_column_specs}
  end

  defp startup_connection(sock, options) do
    payload =
      Frame.new(:startup)
      |> Protocol.encode_request(options)
      |> Frame.encode()

    case :gen_tcp.send(sock, payload) do
      :ok ->
        {:ok, %{body: <<>>}} = recv(sock)
        :ok
      {:error, _reason} = error ->
        error
    end
  end

  defp request_options(sock) do
    payload =
      Frame.new(:options)
      |> Protocol.encode_request(nil)
      |> Frame.encode()

    case :gen_tcp.send(sock, payload) do
      :ok ->
        with {:ok, frame} <- recv(sock),
             do: {:ok, Protocol.decode_response(frame)}
      {:error, _reason} = error ->
        error
    end
  end

  defp register_to_events(sock) do
    payload =
      Frame.new(:register)
      |> Protocol.encode_request(["SCHEMA_CHANGE"])
      |> Frame.encode()

    case :gen_tcp.send(sock, payload) do
      :ok ->
        with {:ok, frame} <- recv(sock) do
          :ok = Protocol.decode_response(frame)
        end
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp recv(sock) do
    length = Frame.header_length()
    with {:ok, header} <- :gen_tcp.recv(sock, length) do
      case Frame.body_length(header) do
        0 ->
          {:ok, Frame.decode(header)}
        length ->
          with {:ok, body} <- :gen_tcp.recv(sock, length) do
            {:ok, Frame.decode(header, body)}
          end
      end
    end
  end

  defp next_frame(data) do
    header_length = Frame.header_length()

    case data do
      <<header::size(header_length)-bytes, rest::binary>> ->
        case Frame.body_length(header) do
          0 ->
            {:ok, Frame.decode(header)}
          length ->
            case rest do
              <<body::size(length)-bytes>> ->
                {:ok, Frame.decode(header, body)}
              _ ->
                {:error, :bad_frame}
            end
        end
      _ ->
        {:error, :bad_frame}
    end
  end

  defp fetch_all_column_specs(db_connection) do
    statement = "SELECT keyspace_name, table_name, column_name, type FROM system_schema.columns"
    with {:ok, %Rows{content: specs}} <- Xandra.execute(db_connection, statement, []),
         do: {:ok, parse_column_specs(specs)}
  end

  defp fetch_column_specs_for_table(db_connection, keyspace, table) do
    statement = """
    SELECT keyspace_name, table_name, column_name, type
    FROM system_schema.columns
    WHERE keyspace_name = ? AND table_name = ?
    """
    with {:ok, %Rows{content: specs}} <- Xandra.execute(db_connection, statement, [keyspace, table]),
         do: {:ok, parse_column_specs(specs)}
  end

  defp parse_column_specs(column_specs) do
    Enum.map(column_specs, fn [keyspace, table, column, string_type] ->
      {keyspace, table, column, TypeParser.parse(string_type)}
    end)
  end
end
