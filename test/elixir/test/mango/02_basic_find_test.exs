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

end
