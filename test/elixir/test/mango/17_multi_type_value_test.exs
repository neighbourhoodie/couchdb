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

defmodule MultiValueFieldTests do
  use CouchTestCase
  defmacro describe(db) do
    quote do
      test "can query with name" do
        selector = %{"name" => %{"$exists" => true}}
        {:ok, docs} = MangoDatabase.find(unquote(db), selector)

        assert length(docs) == 4
        for doc <- docs do
          assert Map.has_key?(doc, "name")
        end
      end

      test "can query with name subfield" do
        selector = %{"name.forename" => %{"$exists" => true}}
        {:ok, docs} = MangoDatabase.find(unquote(db), selector)

        assert length(docs) == 1
        assert Enum.at(docs, 0)["_id"] == "2"
      end

      test "can query with name range" do
        selector = %{"name" => %{"$gte" => 0}}
        {:ok, docs} = MangoDatabase.find(unquote(db), selector)

        assert length(docs) == 3
        for doc <- docs do
          assert Map.has_key?(doc, "name")
          assert doc["name"] in [1, "Jimi", %{"forename" => "Eddie"}]
        end
      end

      test "can query with age and name range" do
        selector = %{"age" => %{"$gte" => 0, "$lt" => 40}, "name" => %{"$gte" => 0}}
        {:ok, docs} = MangoDatabase.find(unquote(db), selector)

        assert length(docs) == 2
        for doc <- docs do
          assert Map.has_key?(doc, "name")
          assert doc["name"] in [1, "Jimi", %{"forename" => "Eddie"}]
        end
      end
    end
  end
end

defmodule MultiValueFieldTestData do
  def docs do
    [
      %{"_id" => "1", "name" => "Jimi", "age" => 10},
      %{"_id" => "2", "name" => %{"forename" => "Eddie"}, "age" => 20},
      %{"_id" => "3", "name" => nil, "age" => 30},
      %{"_id" => "4", "name" => 1, "age" => 40},
      %{"_id" => "5", "forename" => "Sam", "age" => 50}
    ]
  end
end

defmodule MultiValueFieldJSONTests do
  use CouchTestCase
  require MultiValueFieldTests

  @docs MultiValueFieldTestData.docs()
  @db_name "multi-value-field-json"

  setup do
    MangoDatabase.recreate(@db_name)
    MangoDatabase.save_docs(@db_name, @docs)
    MangoDatabase.create_index(@db_name, ["name"])
    MangoDatabase.create_index(@db_name, ["age", "name"])
    :ok
  end

  MultiValueFieldTests.describe(@db_name)
end

defmodule MultiValueFieldAllDocsTests do
  use CouchTestCase
  require MultiValueFieldTests

  @docs MultiValueFieldTestData.docs()
  @db_name "multi-value-field-all-docs"

  setup do
    MangoDatabase.recreate(@db_name)
    MangoDatabase.save_docs(@db_name, @docs)
    :ok
  end

  MultiValueFieldTests.describe(@db_name)
end
