defmodule Xandra do
  alias __MODULE__.{Connection, Query, Error}

  def start_link(opts \\ []) do
    opts = Keyword.put_new(opts, :host, "127.0.0.1")

    if opts[:event_server] do
      host = to_charlist(opts[:host])
      port = opts[:port] || 9042
      {:ok, server} = Supervisor.start_child(Xandra.EventServersSupervisor, [host, port])
      DBConnection.start_link(Connection, Keyword.put(opts, :event_server, server))
    else
      DBConnection.start_link(Connection, opts)
    end
  end

  def stream!(conn, query, params, opts \\ [])

  def stream!(conn, statement, params, opts) when is_binary(statement) do
    with {:ok, query} <- prepare(conn, statement, opts) do
      stream!(conn, query, params, opts)
    end
  end

  def stream!(conn, %Query{} = query, params, opts) do
    %Xandra.Stream{conn: conn, query: query, params: params, opts: opts}
  end

  def prepare(conn, statement, opts \\ []) when is_binary(statement) do
    DBConnection.prepare(conn, %Query{statement: statement}, opts)
  end

  def execute(conn, statement, params, opts \\ [])

  def execute(conn, statement, params, opts) when is_binary(statement) do
    execute(conn, %Query{statement: statement}, params, opts)
  end

  def execute(conn, %Query{} = query, params, opts) do
    with {:ok, %Error{} = error} <- DBConnection.execute(conn, query, params, opts) do
      {:error, error}
    end
  end

  def prepare_execute(conn, statement, params, opts \\ []) when is_binary(statement) do
    DBConnection.prepare_execute(conn, %Query{statement: statement}, params, opts)
  end
end
