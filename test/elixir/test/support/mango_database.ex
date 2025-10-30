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

defmodule MangoDatabase do
  defp put_unless_nil(map, key, value) do
    case value do
      nil -> map
      val -> Map.put(map, key, val)
    end
  end

  def has_text_service() do
    resp = Couch.get("/")
    "search" in resp.body["features"]
  end

  def recreate(db, opts \\ []) do
    resp = Couch.get("/#{db}")
    if resp.status_code == 200 do
      docs = resp.body["doc_count"] + resp.body["doc_del_count"]
      if docs > 0 do
        delete(db)
        create(db, opts)
      end
    else
      create(db, opts)
    end
  end

  defp create(db, opts) do
    partitioned = Keyword.get(opts, :partitioned, false)
    Couch.put("/#{db}?partitioned=#{partitioned}")
  end

  defp delete(db) do
    Couch.delete("/#{db}")
  end

  # TODO: make this use batches if necessary
  def save_docs(db, docs) do
    resp = Couch.post("/#{db}/_bulk_docs", body: %{"docs" => docs})
  end

  def create_index(db, fields, name) do
    Couch.post("/#{db}/_index", body: %{
      "index" => %{"fields" => fields},
      "name" => name,
      "ddoc" => name,
      "type" => "json",
      "w" => 3
    })
  end

  def create_text_index(db) do
    Couch.post("/#{db}/_index", body: %{
      "index" => %{},
      "type" => "text",
      "w" => 3
    })
  end

  # TODO: port more options from src/mango/test/mango.py `def find(...)`
  def find(db, selector, opts \\ []) do
    defaults = [use_index: nil, skip: 0, limit: 25, r: 1, conflicts: false]
    options = Keyword.merge(defaults, opts)

    resp = Couch.post("/#{db}/_find", body: %{
      "selector" => selector,
      "use_index" => options[:use_index],
      "skip" => options[:skip],
      "limit" => options[:limit],
      "r" => options[:r],
      "conflicts" => options[:conflicts]
    }
    |> put_unless_nil("sort", options[:sort]))

    case resp.status_code do
      200 -> {:ok, resp.body["docs"]}
      _ -> {:error, resp}
    end
  end
end
