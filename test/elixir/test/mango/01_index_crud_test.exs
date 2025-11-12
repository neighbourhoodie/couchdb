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

# DOCS = [
#   %{"_id" => "1", "name" => "Jimi", "age" => 10, "cars" => 1},
#   %{"_id" => "2", "name" => "kate", "age" => 8, "cars" => 0}
# ]

defmodule IndexCrudTests do
  use CouchTestCase

  @db_name "index-crud"

  setup do
    MangoDatabase.create(@db_name)
    :ok
  end

  test "bad fields" do
    bad_fields = [
      nil,
      true,
      false,
      "bing",
      2.0,
      %{"foo" => "bar"},
      [%{"foo" => 2}],
      [%{"foo" => "asc", "bar" => "desc"}],
      [%{"foo" => "asc"}, %{"bar" => "desc"}],
      [""],
    ]

    for bad_field <- bad_fields do
      {:error, resp} = MangoDatabase.create_index(@db_name, bad_field)
      assert resp.status_code == 400
    end
  end

  test "bad types" do
    bad_types = [
      nil,
      true,
      false,
      1.5,
      "foo",  # Future support
      "geo",  # Future support
      %{"foo" => "bar"},
      ["baz", 3.0]
    ]

    for bad_type <- bad_types do
      {:error, resp} = MangoDatabase.create_index(
        @db_name,
        ["foo"],
        name: "bad field",
        idx_type: bad_type
      )
      assert resp.status_code == 400
    end
  end

  test "bad names" do
    bad_names = ["", true, false, 1.5, %{"foo" => "bar"}, [nil, false]]

    for bad_name <- bad_names do
      {:error, resp} = MangoDatabase.create_index(
        @db_name,
        ["foo"],
        name: bad_name,
      )
      assert resp.status_code == 400
    end
  end

  test "bad ddocs" do
    bad_ddocs = [
      "",
      "_design/",
      true,
      false,
      1.5,
      %{"foo" => "bar"},
      [nil, false]
    ]

    for bad_ddoc <- bad_ddocs do
      {:error, resp} = MangoDatabase.create_index(
        @db_name,
        ["foo"],
        ddoc: bad_ddoc,
      )
      assert resp.status_code == 400
    end
  end

  test "bad urls" do
    # These are only the negative test cases because ideally the
    # positive ones are implicitly tested by other ones.
    bad_ddocs = [
      "",
      "_design/",
      true,
      false,
      1.5,
      %{"foo" => "bar"},
      [nil, false]
    ]

    for bad_ddoc <- bad_ddocs do
      {:error, resp} = MangoDatabase.create_index(
        @db_name,
        ["foo"],
        ddoc: bad_ddoc,
      )
      assert resp.status_code == 400
    end
  end
end
