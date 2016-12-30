defmodule Xandra.Application do
  use Application

  def start(_type, _args) do
    import Supervisor.Spec
    Supervisor.start_link([worker(Xandra.EventServer, [], restart: :transient)],
                          strategy: :simple_one_for_one,
                          name: Xandra.EventServersSupervisor)
  end
end
