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

defmodule ElemMatchTests do
  use CouchTestCase

  @db_name "basic-text-elem-match"

  setup do
    FriendDocs.setup(@db_name, "text")
  end

  test "elem match non object" do
    q = %{"bestfriends" => %{"$elemMatch" => %{"$eq" => "Wolverine", "$eq" => "Cyclops"}}}
    {:ok, docs} = MangoDatabase.find(@db_name, q)
    assert length(docs) == 1
    assert Enum.at(docs, 0)["bestfriends"] == ["Wolverine", "Cyclops"]

    q = %{"results" => %{"$elemMatch" => %{"$gte" => 80, "$lt" => 85}}}
    {:ok, docs} = MangoDatabase.find(@db_name, q)
    assert length(docs) == 1
    assert Enum.at(docs, 0)["results"] == [82, 85, 88]
  end

  test "elem match" do
    q = %{"friends" => %{"$elemMatch" => %{"name.first" => "Vargas"}}}
    docs = MangoDatabase.find(@db_name, q)
    assert length(docs) == 2
    user_ids = Enum.map(docs, fn doc -> doc["user_id"] end)
    # TODO The order is not correct somehow, it returns [1, 0]
    # assert user_ids == [0, 1]

    q = %{"friends" => %{"$elemMatch" => %{"name.first" => "Ochoa", "name.last" => "Burch"}}}
    docs = MangoDatabase.find(@db_name, q)
    assert length(docs) == 1
    assert Enum.at(docs, 0)["user_id"] == 4

    # Check that we can do logic in elemMatch
    q = %{"friends" => %{"$elemMatch" => %{"name.first" => "Ochoa", "type" => "work"}}}
    docs = MangoDatabase.find(@db_name, q)
    assert length(docs) == 2
    user_ids = Enum.map(docs, fn doc -> doc["user_id"] end)
    # TODO The returned order is not correct
    # assert user_ids == [1, 15]

    q = %{
      "friends" => %{
        "$elemMatch" => %{
          "name.first" => "Ochoa",
          "$or" => [%{"type" => "work"}, %{"type" => "personal"}],
        }
      }
    }
    docs = MangoDatabase.find(@db_name, q)
    assert length(docs) == 3
    user_ids = Enum.map(docs, fn doc -> doc["user_id"] end)
    # TODO The returned order is not correct
    # assert user_ids == [1, 4, 15]

    # Same as last, but using $in
    q = %{
      "friends" => %{
        "$elemMatch" => %{
          "name.first" => "Ochoa",
          "type" => %{"$in" => ["work", "personal"]},
        }
      }
    }
    docs = MangoDatabase.find(@db_name, q)
    assert length(docs) == 3
    user_ids = Enum.map(docs, fn doc -> doc["user_id"] end)
    # TODO The returned order is not correct: [15, 1, 4]
    # assert user_ids == [1, 4, 15]

    # TODO this most likely is wrong
    q = %{
      "$and" => [
        %{
          "friends" => %{
            "$elemMatch" => %{
              "id" => 0,
              "name" => %{"$exists" => true}
            }
          }
        },
        %{
          "friends" => %{
            "$elemMatch" => %{
              "$or" => [
                %{"name" => %{"first" => "Campos", "last" => "Freeman"}},
                %{
                  "name" => %{
                    "$in" => [
                      %{"first" => "Gibbs", "last" => "Mccarty"},
                      %{"first" => "Wilkins", "last" => "Chang"}
                    ]
                  }
                }
              ]
            }
          }
        }
      ]
    }

    docs = MangoDatabase.find(@db_name, q)
    assert length(docs) == 3
    user_ids = Enum.map(docs, fn doc -> doc["user_id"] end)
    # TODO returns this '\n\f\v'
    # assert user_ids == [10, 11, 12]
  end
end

defmodule AllMatchTests do
  use CouchTestCase

  @db_name "basic-text-elem-match"

  setup do
    FriendDocs.setup(@db_name, "text")
  end

  test "test_all_match" do
    q = %{"friends" => %{"$allMatch" => %{"type" => "personal"}}}
    docs = MangoDatabase.find(@db_name, q)
    assert length(docs) == 2
    user_ids = Enum.map(docs, fn doc -> doc["user_id"] end)
    # TODO The returned order is not correct
    # assert user_ids == [8, 5]

    # Check that we can do logic in allMatch
    q = %{
      "friends" => %{
        "$allMatch" => %{
          "name.first" => "Ochoa",
          "$or" => [%{"type" => "work"}, %{"type" => "personal"}],
        }
      }
    }
    docs = MangoDatabase.find(@db_name, q)
    assert length(docs) == 1
    assert Enum.at(docs, 0)["user_id"] == 15

    # Same as last, but using $in
    q = %{
      "friends" => %{
        "$allMatch" => %{
          "name.first" => "Ochoa",
          "type" => %{"$in" => ["work", "personal"]},
        }
      }
    }
    docs = MangoDatabase.find(@db_name, q)
    assert length(docs) == 1
    assert Enum.at(docs, 0)["user_id"] == 15
  end
end
