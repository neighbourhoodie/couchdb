# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

defmodule BasicFindTest do
  use CouchTestCase

  @db_name "basic-find"

  setup do
    UserDocs.setup(@db_name)
  end

  test "simple find" do
    {:ok, docs} = MangoDatabase.find(@db_name, %{"age" => %{"$lt" => 35}})
    user_ids = Enum.map(docs, fn doc -> doc["user_id"] end)

    assert user_ids == [9, 1, 7]
  end

  test "bad selector" do
    bad_selectors = [
      nil,
      true,
      false,
      1.0,
      "foobarbaz",
      %{"foo" => %{"$not_an_op" => 2}},
      %{"$gt" => 2},
      [nil, "bing"],
      %{"_id" => %{"" => None}},
    ]
    Enum.each(bad_selectors, fn bs ->
      {:error, resp} = MangoDatabase.find(@db_name, bs)
      assert resp.status_code == 400
    end)
  end

  test "bad limit" do
    bad_limits = [nil, true, false, -1, 1.2, "no limit!", %{"foo" => "bar"}, [2]]
    Enum.each(bad_limits, fn bl ->
      {:error, resp} = MangoDatabase.find(@db_name, %{"int" => %{"$gt" => 2}}, limit: bl)
      assert resp.status_code == 400
    end)
  end

  test "bad skip" do
    bad_skips = [nil, true, false, -3, 1.2, "no limit!", %{"foo" => "bar"}, [2]]
    Enum.each(bad_skips, fn bs ->
      {:error, resp} = MangoDatabase.find(@db_name, %{"int" => %{"$gt" => 2}}, skip: bs)
      assert resp.status_code == 400
    end)
  end

  test "bad sort" do
    bad_sorts = [
      nil,
      true,
      false,
      1.2,
      "no limit!",
      %{"foo" => "bar"},
      [2],
      [%{"foo" => "asc", "bar" => "asc"}],
      [%{"foo" => "asc"}, %{"bar" => "desc"}]
    ]
    Enum.each(bad_sorts, fn bs ->
      {:error, resp} = MangoDatabase.find(@db_name, %{"int" => %{"$gt" => 2}}, sort: bs)
      assert resp.status_code == 400
    end)
  end

  test "bad fields" do
    bad_fields = [
      nil,
      true,
      false,
      1.2,
      "no limit!",
      %{"foo" => "bar"},
      [2],
      [[]],
      ["foo", 2.0],
    ]
    Enum.each(bad_fields, fn bf ->
      {:error, resp} = MangoDatabase.find(@db_name, %{"int" => %{"$gt" => 2}}, fields: bf)
      assert resp.status_code == 400
    end)
  end

  test "bad r" do
    bad_rs = [nil, true, false, 1.2, "no limit!", %{"foo" => "bar"}, [2]]
    Enum.each(bad_rs, fn br ->
      {:error, resp} = MangoDatabase.find(@db_name, %{"int" => %{"$gt" => 2}}, r: br)
      assert resp.status_code == 400
    end)
  end

  test "bad conflicts" do
    bad_conflicts = [nil, 1.2, "no limit!", %{"foo" => "bar"}, [2]]
    Enum.each(bad_conflicts, fn bc ->
      {:error, resp} = MangoDatabase.find(@db_name, %{"int" => %{"$gt" => 2}}, conflicts: bc)
      assert resp.status_code == 400
    end)
  end

  test "multi cond and" do
    {:ok, docs} = MangoDatabase.find(@db_name, %{"manager" => true, "location.city" => "Longbranch"})

    user_id = Enum.map(docs, fn doc -> doc["user_id"] end)
    assert user_id == [7]
  end

  test "multi cond duplicate field" do
    # need to explicitly define JSON as dict won't allow duplicate keys
    body = %{
      "location.city" => %{"$regex" => "^L+"},
      "location.city" => %{"$exists" => true}
    }
    {:ok, docs} = MangoDatabase.find(@db_name, body)
    # expectation is that only the second instance
    # of the "location.city" field is used
    assert length(docs) == 15
  end

  test "multi cond or" do
    {:ok, docs} = MangoDatabase.find(
      @db_name,
      %{
        "$and" => [
          %{"age" => %{"$gte" => 75}},
          %{"$or" => [%{"name.first" => "Mathis"}, %{"name.first" => "Whitley"}]},
        ]
      }
    )

    user_id = Enum.map(docs, fn doc -> doc["user_id"] end)
    assert user_id == [11, 13]
  end

  test "multi col idx" do
    {:ok, docs} = MangoDatabase.find(
      @db_name,
        %{
          "location.state" => %{"$and" => [%{"$gt" => "Hawaii"}, %{"$lt" => "Maine"}]},
          "location.city" => %{"$lt" => "Longbranch"},
        }
    )

    user_id = Enum.map(docs, fn doc -> doc["user_id"] end)
    assert user_id == [6]
  end

end
