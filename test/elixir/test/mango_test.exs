defmodule MangoTest do
  use CouchTestCase

  @moduletag :mango

  @moduledoc """
  Test CouchDB Mango.
  This is a port of the Mango Python test suite suite
  """

  test "Mango Smoke Test" do
    user_ctx = Couch.get("/_session").body["userCtx"]
    assert user_ctx["name"] == "adm", "Should have adm user context"
    assert user_ctx["roles"] == ["_admin"], "Should have _admin role"
  end

  @tag :with_db
  test "Mango Basic Find: bad selector", ctx do
    db_name = ctx[:db_name]
    bad_selectors = [
      %{"field" => %{"$gt" => 2}}
      # :nil,
      # :true,
      # :false,
      # 1.0,
      # "foobarbaz",
      # %{"foo" => %{"$not_an_op" => 2}},
      # %{"$gt" => 2},
      # [:null, "bing"],
      # %{"_id" => %{"" => :null}}
    ]
    # Enum.each(["some", "example"], fn x -> IO.puts(x) end)
    Enum.each(bad_selectors, fn selector ->
      IO.inspect(selector)
      response = Couch.post("/#{db_name}/_find", body: selector)
      IO.inspect(response)
    end)
  end
end
