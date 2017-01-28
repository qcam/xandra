defmodule Xandra.RetryPolicy do
  @callback new(list) :: {:ok, any} | {:error, Exception.t}
  @callback handle_retry(atom, String.t, atom, any) :: {:retry, atom, any} | :ignore
end

defmodule Xandra.RetryPolicy.Counter do
  def new(options) do
    with :error <- Keyword.fetch(options, :retry_count) do
      {:error, "expected the :retry_count option to be specified"}
    end
  end

  def handle_retry(_reason, _consistency, 0) do
    :ignore
  end

  def handle_retry(_reason, consistency, retry_count) do
    {:retry, consistency, retry_count - 1}
  end
end
