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

# TODO module TextIndexCheckTests

defmodule BasicTextTests do
  use CouchTestCase

  @db_name "basic-text-elem-match"

  setup do
    UserDocs.setup(@db_name, "text")
  end

  test "test_simple" do
    docs = MangoDatabase.find(@db_name, %{"$text" => "Stephanie"})
    # TODO it returns 15 docs
    # assert length(docs) == 1
    # assert Enum.at(docs, 0)["name"]["first"] == "Stephanie"
  end

  test "test_with_integer" do
    docs = MangoDatabase.find(@db_name, %{"name.first" => "Stephanie", "age" => 48})
    assert length(docs) == 1
    assert Enum.at(docs, 0)["name"]["first"] == "Stephanie"
    assert Enum.at(docs, 0)["age"] == 48
  end

  test "test_with_boolean" do
    docs = MangoDatabase.find(@db_name, %{"name.first" => "Stephanie", "manager" => false})
    assert length(docs) == 1
    assert Enum.at(docs, 0)["name"]["first"] == "Stephanie"
    assert Enum.at(docs, 0)["manager"] == false
  end

  test "test_with_array" do
    faves = ["Ruby", "C", "Python"]
    docs = MangoDatabase.find(@db_name, %{"name.first" => "Stephanie", "favorites" => faves})
    assert Enum.at(docs, 0)["name"]["first"] == "Stephanie"
    assert Enum.at(docs, 0)["favorites"] == faves
  end

  test "test_array_ref" do
    docs = MangoDatabase.find(@db_name, %{"favorites.1" => "Python"})
    assert length(docs) == 4
    assert Enum.all?(Enum.map(docs, & &1["favorites"]), fn fav -> Enum.member?(fav, "Python") end)

    # Nested Level
    docs = MangoDatabase.find(@db_name, %{"favorites.0.2" => "Python"})
    assert length(docs) == 1
    nested_favorite = Enum.map(docs, fn d -> d["favorites"] |> Enum.at(0) |> Enum.at(2) end)
    assert Enum.at(nested_favorite, 0) == "Python"
  end

  test "test_number_ref" do
    docs = MangoDatabase.find(@db_name, %{"11111" => "number_field"})
    assert length(docs) == 1
    assert Enum.at(docs, 0)["11111"] == "number_field"

    docs = MangoDatabase.find(@db_name, %{"22222.33333" => "nested_number_field"})
    assert length(docs) == 1
    assert Enum.at(docs, 0)["22222"]["33333"] == "nested_number_field"
  end

  test "test_lt" do
    docs = MangoDatabase.find(@db_name, %{"age" => %{"$lt" => 22}})
    assert length(docs) == 0

    docs = MangoDatabase.find(@db_name, %{"age" => %{"$lt" => 23}})
    assert length(docs) == 1
    assert Enum.at(docs, 0)["user_id"] == 9

    docs = MangoDatabase.find(@db_name, %{"age" => %{"$lt" => 33}})
    user_ids = Enum.map(docs, fn doc -> doc["user_id"] end)
    assert Enum.sort(user_ids) == [1, 9]

    docs = MangoDatabase.find(@db_name, %{"age" => %{"$lt" => 34}})
    assert length(docs) == 3

    user_ids = Enum.map(docs, fn doc -> doc["user_id"] end)
    assert Enum.sort(user_ids) == [1, 7, 9]

    docs = MangoDatabase.find(@db_name, %{"company" => %{"$lt" => "Dreamia"}})
    assert length(docs) == 1
    assert Enum.at(docs, 0)["company"] == "Affluex"

    docs = MangoDatabase.find(@db_name, %{"foo" => %{"$lt" => "bar car apple"}})
    assert length(docs) == 0
  end

  test "test_lte" do
    docs = MangoDatabase.find(@db_name, %{"age" => %{"$lte" => 21}})
    assert length(docs) == 0

    docs = MangoDatabase.find(@db_name, %{"age" => %{"$lte" => 22}})
    assert length(docs) == 1
    assert Enum.at(docs, 0)["user_id"] == 9

    docs = MangoDatabase.find(@db_name, %{"age" => %{"$lte" => 33}})
    user_ids = Enum.map(docs, fn doc -> doc["user_id"] end)
    assert Enum.sort(user_ids) == [1, 7, 9]

    docs = MangoDatabase.find(@db_name, %{"company" => %{"$lte" => "Dreamia"}})
    user_ids = Enum.map(docs, fn doc -> doc["user_id"] end)
    assert user_ids == [0, 11]

    docs = MangoDatabase.find(@db_name, %{"foo" => %{"$lte" => "bar car apple"}})
    assert length(docs) == 1
    assert Enum.at(docs, 0)["user_id"] == 14
  end

  test "test_eq" do
    docs = MangoDatabase.find(@db_name, %{"age" => 21})
    assert length(docs) == 0

    docs = MangoDatabase.find(@db_name, %{"age" => 22})
    assert length(docs) == 1
    assert Enum.at(docs, 0)["user_id"] == 9

    docs = MangoDatabase.find(@db_name, %{"age" => %{"$eq" => 22}})
    assert length(docs) == 1
    assert Enum.at(docs, 0)["user_id"] == 9

    docs = MangoDatabase.find(@db_name, %{"age" => 33})
    assert length(docs) == 1
    assert Enum.at(docs, 0)["user_id"] == 7
  end

  test "test_ne" do
    docs = MangoDatabase.find(@db_name, %{"age" => %{"$ne" => 22}})
    assert length(docs) == UserDocs.get_docs_length() - 1
    assert Enum.all?(docs, fn doc -> doc["age"] != 22 end)

    docs = MangoDatabase.find(@db_name, %{"$not" => %{"age" => 22}})
    assert length(docs) == UserDocs.get_docs_length() - 1
    assert Enum.all?(docs, fn doc -> doc["age"] != 22 end)
  end

  test "test_gt" do
    docs = MangoDatabase.find(@db_name, %{"age" => %{"$gt" => 77}})
    user_ids = Enum.map(docs, fn doc -> doc["user_id"] end)
    assert Enum.sort(user_ids) == [3, 13]

    docs = MangoDatabase.find(@db_name, %{"age" => %{"$gt" => 78}})
    assert length(docs) == 1
    assert Enum.at(docs, 0)["user_id"] == 3

    docs = MangoDatabase.find(@db_name, %{"age" => %{"$gt" => 79}})
    assert length(docs) == 0

    docs = MangoDatabase.find(@db_name, %{"company" => %{"$gt" => "Zialactic"}})
    assert length(docs) == 0

    docs = MangoDatabase.find(@db_name, %{"foo" => %{"$gt" => "bar car apple"}})
    assert length(docs) == 0

    docs = MangoDatabase.find(@db_name, %{"foo" => %{"$gt" => "bar car"}})
    assert length(docs) == 1
    assert Enum.at(docs, 0)["user_id"] == 14
  end

  test "test_gte" do
    docs = MangoDatabase.find(@db_name, %{"age" => %{"$gte" => 77}})
    user_ids = Enum.map(docs, fn doc -> doc["user_id"] end)
    assert Enum.sort(user_ids) == [3, 13]

    docs = MangoDatabase.find(@db_name, %{"age" => %{"$gte" => 78}})
    user_ids = Enum.map(docs, fn doc -> doc["user_id"] end)
    assert Enum.sort(user_ids) == [3, 13]

    docs = MangoDatabase.find(@db_name, %{"age" => %{"$gte" => 79}})
    assert length(docs) == 1
    assert Enum.at(docs, 0)["user_id"] == 3

    docs = MangoDatabase.find(@db_name, %{"age" => %{"$gte" => 80}})
    assert length(docs) == 0

    docs = MangoDatabase.find(@db_name, %{"company" => %{"$gte" => "Zialactic"}})
    assert length(docs) == 1
    assert Enum.at(docs, 0)["company"] == "Zialactic"

    docs = MangoDatabase.find(@db_name, %{"foo" => %{"$gte" => "bar car apple"}})
    assert length(docs) == 1
    assert Enum.at(docs, 0)["user_id"] == 14
  end

  test "test_and" do
    docs = MangoDatabase.find(@db_name, %{"age" => 22, "manager" => true})
    assert length(docs) == 1
    assert Enum.at(docs, 0)["user_id"] == 9

    docs = MangoDatabase.find(@db_name, %{"age" => 22, "manager" => false})
    assert length(docs) == 0

    docs = MangoDatabase.find(@db_name, %{"$and" => [%{"age" => 22}, %{"manager" => true}]})
    assert length(docs) == 1
    assert Enum.at(docs, 0)["user_id"] == 9

    docs = MangoDatabase.find(@db_name, %{"$and" => [%{"age" => 22}, %{"manager" => false}]})
    assert length(docs) == 0

    docs = MangoDatabase.find(@db_name, %{"$text" => "Ramona", "age" => 22})
    assert length(docs) == 1
    assert Enum.at(docs, 0)["user_id"] == 9

    docs = MangoDatabase.find(@db_name, %{"$and" => [%{"$text" => "Ramona"}, %{"age" => 22}]})
    assert length(docs) == 1
    assert Enum.at(docs, 0)["user_id"] == 9

    docs = MangoDatabase.find(@db_name, %{"$and" => [%{"$text" => "Ramona"}, %{"$text" => "Floyd"}]})
    # TODO FIX
    # assert length(docs) == 1
    # assert Enum.at(docs, 0)["user_id"] == 9
  end

  test "test_or" do
    docs = MangoDatabase.find(@db_name, %{"$or" => [%{"age" => 22}, %{"age" => 33}]})
    user_ids = Enum.map(docs, fn doc -> doc["user_id"] end)
    assert Enum.sort(user_ids) == [7, 9]

    q = %{"$or" => [%{"$text" => "Ramona"}, %{"$text" => "Stephanie"}]}
    docs = MangoDatabase.find(@db_name, q)
    user_ids = Enum.map(docs, fn doc -> doc["user_id"] end)
    # TODO returns 9 files
    # assert Enum.sort(user_ids) == [0, 9]

    q = %{"$or" => [%{"$text" => "Ramona"}, %{"age" => 22}]}
    docs = MangoDatabase.find(@db_name, q)
    # TODO fails returns 15 files
    # assert length(docs) == 1
    # assert Enum.at(docs, 0)["user_id"] == 9
  end

  test "test_and_or" do
    q = %{"age" => 22, "$or" => [%{"manager" => false}, %{"location.state" => "Missouri"}]}
    docs = MangoDatabase.find(@db_name, q)
    assert length(docs) == 1
    assert Enum.at(docs, 0)["user_id"] == 9

    q = %{"$or" => [%{"age" => 22}, %{"age" => 43, "manager" => true}]}
    docs = MangoDatabase.find(@db_name, q)
    user_ids = Enum.map(docs, fn doc -> doc["user_id"] end)
    assert Enum.sort(user_ids) == [9, 10]

    q = %{"$or" => [%{"$text" => "Ramona"}, %{"age" => 43, "manager" => true}]}
    docs = MangoDatabase.find(@db_name, q)
    # TODO fails returns 15 files
    # user_ids = Enum.map(docs, fn doc -> doc["user_id"] end)
    # assert Enum.sort(user_ids) == [9, 10]
  end

  test "test_nor" do
    docs = MangoDatabase.find(@db_name, %{"$nor" => [%{"age" => 22}, %{"age" => 33}]})
    assert length(docs) == 13
    assert Enum.all?(docs, fn doc ->
      not Enum.member?([7, 9], doc["user_id"])
    end)
  end

  test "test_in_with_value" do
    docs = MangoDatabase.find(@db_name, %{"age" => %{"$in" => [1, 5]}})
    assert length(docs) == 0

    docs = MangoDatabase.find(@db_name, %{"age" => %{"$in" => [1, 5, 22]}})
    assert length(docs) == 1
    assert Enum.at(docs, 0)["user_id"] == 9

    docs = MangoDatabase.find(@db_name, %{"age" => %{"$in" => [1, 5, 22, 31]}})
    user_ids = Enum.map(docs, fn doc -> doc["user_id"] end)
    assert Enum.sort(user_ids) == [1, 9]

    docs = MangoDatabase.find(@db_name, %{"age" => %{"$in" => [22, 31]}})
    user_ids = Enum.map(docs, fn doc -> doc["user_id"] end)
    assert Enum.sort(user_ids) == [1, 9]

    # Limits on boolean clauses?
    docs = MangoDatabase.find(@db_name, %{"age" => %{"$in" => Enum.to_list(0..999)}})
    assert length(docs) == 15
  end

  test "test_in_with_array" do
    vals = ["Random Garbage", 52, %{"Versions" => %{"Alpha" => "Beta"}}]
    docs = MangoDatabase.find(@db_name, %{"favorites" => %{"$in" => vals}})
    assert length(docs) == 1
    assert Enum.at(docs, 0)["user_id"] == 1

    vals = ["Lisp", "Python"]
    docs = MangoDatabase.find(@db_name, %{"favorites" => %{"$in" => vals}})
    assert length(docs) == 10

    vals = [%{"val1" => 1, "val2" => "val2"}]
    docs = MangoDatabase.find(@db_name, %{"test_in" => %{"$in" => vals}})
    assert length(docs) == 1
    assert Enum.at(docs, 0)["user_id"] == 2
  end

  test "test_nin_with_value" do
    docs = MangoDatabase.find(@db_name, %{"age" => %{"$nin" => [1, 5]}})
    assert length(docs) == UserDocs.get_docs_length()

    docs = MangoDatabase.find(@db_name, %{"age" => %{"$nin" => [1, 5, 22]}})
    assert length(docs) == UserDocs.get_docs_length() - 1
    assert Enum.all?(docs, fn doc -> doc["user_id"] != 9 end)

    docs = MangoDatabase.find(@db_name, %{"age" => %{"$nin" => [1, 5, 22, 31]}})
    assert length(docs) == UserDocs.get_docs_length() - 2
    assert Enum.all?(docs, fn doc ->
      not Enum.member?([1, 9], doc["user_id"])
    end)

    docs = MangoDatabase.find(@db_name, %{"age" => %{"$nin" => [22, 31]}})
    assert length(docs) == UserDocs.get_docs_length() - 2
    assert Enum.all?(docs, fn doc ->
      not Enum.member?([1, 9], doc["user_id"])
    end)

    # Limits on boolean clauses?
    docs = MangoDatabase.find(@db_name, %{"age" => %{"$nin" =>  Enum.to_list(0..1000)}})
    assert length(docs) == 0
  end

  test "test_nin_with_array" do
    vals = ["Random Garbage", 52, %{"Versions" => %{"Alpha" => "Beta"}}]
    docs = MangoDatabase.find(@db_name, %{"favorites" => %{"$nin" => vals}})
    assert length(docs) == UserDocs.get_docs_length() - 1
    assert Enum.all?(docs, fn doc -> doc["user_id"] != 1 end)

    vals = ["Lisp", "Python"]
    docs = MangoDatabase.find(@db_name, %{"favorites" => %{"$nin" => vals}})
    assert length(docs) == 5

    vals = [%{"val1" => 1, "val2" => "val2"}]
    docs = MangoDatabase.find(@db_name, %{"test_in" => %{"$nin" => vals}})
    assert length(docs) == 0
  end

  test "test_all" do
    vals = ["Ruby", "C", "Python", %{"Versions" => %{"Alpha" => "Beta"}}]
    docs = MangoDatabase.find(@db_name, %{"favorites" => %{"$all" => vals}})
    assert length(docs) == 1
    assert Enum.at(docs, 0)["user_id"] == 1

    # This matches where favorites either contains
    # the nested array, or is the nested array. This is
    # notably different than the non-nested array in that
    # it does not match a re-ordered version of the array.
    # The fact that user_id 14 isn't included demonstrates
    # this behavior.
    vals = [["Lisp", "Erlang", "Python"]]
    docs = MangoDatabase.find(@db_name, %{"favorites" => %{"$all" => vals}})
    assert length(docs) == 2
    assert Enum.all?(docs, fn doc ->
      Enum.member?([3, 9], doc["user_id"])
    end)
  end

  test "test_exists_field" do
    docs = MangoDatabase.find(@db_name, %{"exists_field" => %{"$exists" => true}})
    assert length(docs) == 2
    assert Enum.all?(docs, fn doc ->
      Enum.member?([7, 8], doc["user_id"])
    end)

    docs = MangoDatabase.find(@db_name, %{"exists_field" => %{"$exists" => false}})
    assert length(docs) == UserDocs.get_docs_length() - 2
    assert Enum.all?(docs, fn doc ->
      not Enum.member?([7, 8], doc["user_id"])
    end)
  end

  test "test_exists_array" do
    docs = MangoDatabase.find(@db_name, %{"exists_array" => %{"$exists" => true}})
    assert length(docs) == 2
    assert Enum.all?(docs, fn doc ->
      Enum.member?([9, 10], doc["user_id"])
    end)

    docs = MangoDatabase.find(@db_name, %{"exists_array" => %{"$exists" => false}})
    assert length(docs) == UserDocs.get_docs_length() - 2
    assert Enum.all?(docs, fn doc ->
      not Enum.member?([9, 10], doc["user_id"])
    end)
  end

  test "test_exists_object" do
    docs = MangoDatabase.find(@db_name, %{"exists_object" => %{"$exists" => true}})
    assert length(docs) == 2
    assert Enum.all?(docs, fn doc ->
      Enum.member?([11, 12], doc["user_id"])
    end)

    docs = MangoDatabase.find(@db_name, %{"exists_object" => %{"$exists" => false}})
    assert length(docs) == UserDocs.get_docs_length() - 2
    assert Enum.all?(docs, fn doc ->
      not Enum.member?([11, 12], doc["user_id"])
    end)
  end

  test "test_exists_object_member" do
    docs = MangoDatabase.find(@db_name, %{"exists_object.should" => %{"$exists" => true}})
    assert length(docs) == 1
    assert Enum.at(docs, 0)["user_id"] == 11

    docs = MangoDatabase.find(@db_name, %{"exists_object.should" => %{"$exists" => false}})
    assert length(docs) == UserDocs.get_docs_length() - 1
    assert Enum.all?(docs, fn doc -> doc["user_id"] != 11 end)
  end

  test "test_exists_and" do
    q = %{
      "$and" => [
        %{"manager" => %{"$exists" => true}},
        %{"exists_object.should" => %{"$exists" => true}},
      ]
    }
    docs = MangoDatabase.find(@db_name, q)
    assert length(docs) == 1
    assert Enum.at(docs, 0)["user_id"] == 11

    q = %{
      "$and" => [
        %{"manager" => %{"$exists" => false}},
        %{"exists_object.should" => %{"$exists" => true}},
      ]
    }
    docs = MangoDatabase.find(@db_name, q)
    assert length(docs) == 0

    # Translates to manager exists or exists_object.should doesn't
    # exist, which will match all docs
    q = %{"$not" => q}
    docs = MangoDatabase.find(@db_name, q)
    assert length(docs) == UserDocs.get_docs_length()
  end

  # TODO charlist fails
  # test "test_value_chars" do
  #   q = %{"complex_field_value" => '+-(){}[]^~&&*||"\\/?:!'}
  #   docs = MangoDatabase.find(@db_name, q)
  #   assert length(docs) == 1
  # end

  test "test_regex" do
    docs = MangoDatabase.find(@db_name,
      %{"age" => %{"$gt" => 40}, "location.state" => %{"$regex" => "(?i)new.*"}}
    )
    assert length(docs) == 2
    assert Enum.at(docs, 0)["user_id"] == 2
    assert Enum.at(docs, 1)["user_id"] == 10
  end
end

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
    user_ids = Enum.map(docs, fn doc -> doc["user_id"] end)
    assert Enum.sort(user_ids) == [0, 1]

    q = %{"friends" => %{"$elemMatch" => %{"name.first" => "Ochoa", "name.last" => "Burch"}}}
    docs = MangoDatabase.find(@db_name, q)
    assert length(docs) == 1
    assert Enum.at(docs, 0)["user_id"] == 4

    # Check that we can do logic in elemMatch
    q = %{"friends" => %{"$elemMatch" => %{"name.first" => "Ochoa", "type" => "work"}}}
    docs = MangoDatabase.find(@db_name, q)
    user_ids = Enum.map(docs, fn doc -> doc["user_id"] end)
    assert Enum.sort(user_ids) == [1, 15]

    q = %{
      "friends" => %{
        "$elemMatch" => %{
          "name.first" => "Ochoa",
          "$or" => [%{"type" => "work"}, %{"type" => "personal"}],
        }
      }
    }
    docs = MangoDatabase.find(@db_name, q)
    user_ids = Enum.map(docs, fn doc -> doc["user_id"] end)
    assert Enum.sort(user_ids) == [1, 4, 15]

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
    user_ids = Enum.map(docs, fn doc -> doc["user_id"] end)
    assert Enum.sort(user_ids) == [1, 4, 15]

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
    user_ids = Enum.map(docs, fn doc -> doc["user_id"] end)
    assert Enum.sort(user_ids) == [10, 11, 12]
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
    user_ids = Enum.map(docs, fn doc -> doc["user_id"] end)
    assert Enum.sort(user_ids) == [5, 8]

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

# TODO module NumStringTests
