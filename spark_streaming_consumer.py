from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, regexp_replace, row_number, trim, when, desc, asc
from pyspark.sql.types import StructType, StringType, DoubleType
from threading import Thread
from flask import Flask, jsonify, request, render_template_string
from pyspark.sql.window import Window
from pyspark.sql.functions import col

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, regexp_replace, row_number, trim, when, desc, asc
from pyspark.sql.types import StructType, StringType, DoubleType
from threading import Thread
from flask import Flask, jsonify, request, render_template_string
from pyspark.sql.window import Window
from langchain_community.chat_models import ChatOpenAI
from langchain.schema import SystemMessage, HumanMessage
import re
import os
from dotenv import load_dotenv
import json

load_dotenv()


# Initialize Spark session
spark = SparkSession.builder \
    .appName("NCAAKafkaConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema for incoming Kafka JSON
schema = StructType().add("stat_category", StringType()) \
                     .add("timestamp", DoubleType()) \
                     .add("player_stats", StringType()) \
                     .add("team_stats", StringType())

# Read stream from Kafka topic
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "ncaa-player-stats") \
    .load()

# Parse JSON payload
json_df = raw_df.selectExpr("CAST(value AS STRING)") \
                .select(from_json("value", schema).alias("data")) \
                .select("data.*")

memory_tables = {}

player_value_key_mapping = {
    "Stolen Bases Per Game": "PG",
    "Batting Average": "BA",
    "Earned Run Average": "ERA",
    "Slugging Percentage": "SLG PCT",
    "WHIP": "WHIP",
    "Home Runs Per Game": "PG",
    "On Base Percentage": "PCT",
    "Complete Games": "CG",
    "Hits Allowed Per Nine Innings": "PG",
    "Strikeouts Per Nine Innings": "K/9",
    "Walks Allowed Per Nine Innings": "PG"
}

team_value_key_mapping = {
    "Stolen Bases Per Game": "PG",
    "Batting Average": "BA",
    "Earned Run Average": "ERA",
    "Slugging Percentage": "SLG PCT",
    "WHIP": "WHIP",
    "WL Pct": "PCT",
    "Home Runs Per Game": "PG",
    "Fielding Percentage": "PCT",
    "On Base Percentage": "PCT",
    "Complete Games": "CG",
    "Hits Allowed Per Nine Innings": "PG",
    "Strikeouts Per Nine Innings": "K/9",
    "Walks Allowed Per Nine Innings": "PG"
}




# Initialize the LLM
llm = ChatOpenAI(
    model_name="gpt-4",  # or "gpt-3.5-turbo"
    temperature=0.3,
    openai_api_key=os.getenv("OPENAI_API_KEY")
)

def call_llm_api(prompt: str) -> str:
    """Send prompt to LLM via LangChain and return response."""
    messages = [
        SystemMessage(content="You are a helpful recruiting assistant analyzing NCAA baseball stats."),
        HumanMessage(content=prompt)
    ]
    try:
        response = llm(messages)
        return response.content
    except Exception as e:
        return f"Error contacting LLM: {e}"

def normalize_table_name(name):
    return re.sub(r'[^a-zA-Z0-9]', '_', name.strip().lower())

def parse_and_update_state(batch_df, epoch_id):
    if batch_df.rdd.isEmpty():
        return

    batch_df.persist()

    if "player_stats" in batch_df.columns:
        player_df = batch_df.filter(col("player_stats").isNotNull())
        categories = player_df.select("stat_category").distinct().collect()
        for row in categories:
            category = row["stat_category"]
            value_field = player_value_key_mapping.get(category, "Value")

            stats_schema = StructType() \
                .add("Name", StringType()) \
                .add("Team", StringType()) \
                .add("Cl", StringType()) \
                .add("Position", StringType())\
                .add(value_field, StringType())

            filtered_df = player_df.filter(col("stat_category") == category)
            parsed_df = filtered_df.withColumn("player_stats_json", from_json("player_stats", stats_schema))

            flattened_df = parsed_df.select(
                col("player_stats_json.Name").alias("Name"),
                col("player_stats_json.Team").alias("Team"),
                col("player_stats_json.Cl").alias("Cl"),
                col("player_stats_json.Position").alias("Position"),
                col(f"player_stats_json.{value_field}").alias("Value")
            )

            table_name = f"individual_{normalize_table_name(category)}"
            if table_name not in memory_tables:
                memory_tables[table_name] = spark.createDataFrame([], flattened_df.schema)

            current_table = memory_tables[table_name]
            if not flattened_df.rdd.isEmpty():
                key_cols = ["Name", "Team"]
                current_table = current_table.join(flattened_df.select(*key_cols).dropDuplicates(), on=key_cols, how="left_anti")
                current_table = current_table.union(flattened_df)

            current_table.createOrReplaceTempView(table_name)
            memory_tables[table_name] = current_table
            print(f"Updated memory table: {table_name}")

    if "team_stats" in batch_df.columns:
        team_df = batch_df.filter(col("team_stats").isNotNull())
        categories = team_df.select("stat_category").distinct().collect()
        for row in categories:
            category = row["stat_category"]
            value_field = team_value_key_mapping.get(category, "Value")

            team_stats_schema = StructType() \
                .add("Team", StringType()) \
                .add(value_field, StringType())

            filtered_df = team_df.filter(col("stat_category") == category)
            parsed_df = filtered_df.withColumn("team_stats_json", from_json("team_stats", team_stats_schema))

            flattened_df = parsed_df.select(
                col("team_stats_json.Team").alias("Team"),
                col(f"team_stats_json.{value_field}").alias("Value")
            )

            table_name = f"team_{normalize_table_name(category)}"
            if table_name not in memory_tables:
                memory_tables[table_name] = spark.createDataFrame([], flattened_df.schema)

            current_table = memory_tables[table_name]
            if not flattened_df.rdd.isEmpty():
                key_cols = ["Team"]
                current_table = current_table.join(flattened_df.select(*key_cols).dropDuplicates(), on=key_cols, how="left_anti")
                current_table = current_table.union(flattened_df)

            current_table.createOrReplaceTempView(table_name)
            memory_tables[table_name] = current_table
            print(f"Updated memory table: {table_name}")

    batch_df.unpersist()

query = json_df.writeStream \
    .foreachBatch(parse_and_update_state) \
    .outputMode("append") \
    .start()

app = Flask(__name__)

RANKING_ORDER_BY_CATEGORY = {
    "Stolen Bases Per Game": "desc",               # more is better
    "Batting Average": "desc",                     # more is better
    "Earned Run Average": "asc",                   # lower is better
    "Slugging Percentage": "desc",                 # more is better
    "WHIP": "asc",                                 # lower is better
    "WL Pct": "desc",                              # more is better
    "Home Runs Per Game": "desc",                  # more is better
    "On Base Percentage": "desc",                  # more is better
    "Complete Games": "desc",                      # more is better
    "Hits Allowed Per Nine Innings": "asc",        # lower is better
    "Strikeouts Per Nine Innings": "desc",         # more is better
    "Walks Allowed Per Nine Innings": "asc",       # lower is better
    "Fielding Percentage": "desc"                  # more is better
}


def metric_name_from_table(table_name):
    base_name = re.sub(r'^(individual_|team_)', '', table_name)
    base_name = base_name.replace("_", " ").title()

    if "Whip" in base_name:
        return "WHIP"
    if "Wl Pct" in base_name or "Wlpct" in base_name.replace(" ", ""):
        return "WL Pct"
    return base_name

def apply_ranking(df, metric_name):
    if metric_name not in RANKING_ORDER_BY_CATEGORY:
        return df
    order = RANKING_ORDER_BY_CATEGORY[metric_name]
    order_func = desc("Value") if order == "desc" else asc("Value")
    window_spec = Window.orderBy(order_func)
    return df.withColumn("Rank", row_number().over(window_spec))



@app.route("/tables")
def list_tables():
    prefix = request.args.get("type", "individual")
    table_names = [name for name in memory_tables.keys() if name.startswith(prefix)]
    return jsonify(sorted(table_names))

@app.route("/tables/<table_name>")
def get_table_data(table_name):
    if table_name not in memory_tables:
        return jsonify([])

    df = memory_tables[table_name]
    metric_name = metric_name_from_table(table_name)

    if metric_name == "WL Pct" and not table_name.startswith("team_"):
        data = df.limit(250).toPandas().to_dict(orient="records")
        return jsonify(data)

    ranked_df = apply_ranking(df, metric_name)
    data = ranked_df.limit(300).toPandas().to_dict(orient="records")
    return jsonify(data)



@app.route("/chat", methods=["POST"])
def handle_chat():
    data = request.json
    query = data.get("query", "").strip()
    stat_type = data.get("type", "").strip().lower()

    if not query or stat_type not in ["individual", "team"]:
        return jsonify({"response": "Please select a valid stat type and ask a question."})

    # Filter relevant tables for stat_type
    relevant_tables = {
        k: v.toPandas().to_dict("records")[:20]
        for k, v in memory_tables.items()
        if k.startswith(stat_type)
    }

    if not relevant_tables:
        return jsonify({"response": f"No data found for {stat_type} stats."})

    # Optional: prioritize tables that match words in the query
    def table_relevance(name): return sum(word in name for word in query.lower().split())
    sorted_tables = dict(sorted(relevant_tables.items(), key=lambda x: -table_relevance(x[0])))

    # Build a trimmed prompt
    prompt_sections = []
    for k, records in list(sorted_tables.items())[:5]:  # limit to 5 tables max
        prompt_sections.append(f"Category: {k.replace('_', ' ').title()}\nData:\n{json.dumps(records, indent=2)}")

    joined_sections = "\n\n".join(prompt_sections)

    prompt = f"""
You are a helpful NCAA D1 baseball recruiter assistant.

A recruiter asked: "{query}"

Here are relevant stats from multiple categories:
{joined_sections}

Answer clearly and professionally using this data.
"""
    try:
        reply = call_llm_api(prompt)
        return jsonify({"response": reply})
    except Exception as e:
        return jsonify({"response": f"LLM error: {str(e)}"})

@app.route("/")
def dashboard():
    html = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Live D1 Baseball Stats Dashboard</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet" />
  <style>
    body { padding: 20px; }
    th { user-select: none; cursor: pointer; }
    #data-table { max-height: 500px; overflow-y: auto; margin-top: 20px; }
    .dropdown-row {
      display: flex;
      gap: 20px;
      flex-wrap: wrap;
      align-items: center;
    }
    .dropdown-row .form-group {
      flex: 1 1 auto;
      max-width: 300px;
    }
  </style>
</head>
<body>
  <h1 class="mb-4">Live D1 Baseball Stats Dashboard</h1>

  <div class="dropdown-row mb-4">
    <div class="form-group">
      <label for="stat-type-select" class="form-label">Stat Type:</label>
      <select id="stat-type-select" class="form-select">
        <option value="individual" selected>Individual</option>
        <option value="team">Team</option>
      </select>
    </div>

    <div class="form-group">
      <label for="category-select" class="form-label">Metric Category:</label>
      <select id="category-select" class="form-select">
        <option selected disabled>Loading categories...</option>
      </select>
    </div>
  </div>

  <!-- Filters for Individual Stats -->
  <div class="dropdown-row mb-4" id="filters-row" style="display: none;">
    <div class="form-group">
      <label for="team-filter" class="form-label">Filter by Team:</label>
      <select id="team-filter" class="form-select">
        <option value="">All</option>
      </select>
    </div>
    <div class="form-group">
      <label for="class-filter" class="form-label">Filter by Cl:</label>
      <select id="class-filter" class="form-select">
        <option value="">All</option>
      </select>
    </div>
    <div class="form-group">
      <label for="pos-filter" class="form-label">Filter by Position:</label>
      <select id="pos-filter" class="form-select">
        <option value="">All</option>
      </select>
    </div>
  </div>

  <h3 id="table-title" class="mt-4"></h3>
  <div id="data-table" class="table-responsive"></div>

  <script>
    const statTypeSelect = document.getElementById('stat-type-select');
    const categorySelect = document.getElementById('category-select');
    const dataTable = document.getElementById('data-table');
    const tableTitle = document.getElementById('table-title');

    const teamFilter = document.getElementById('team-filter');
    const classFilter = document.getElementById('class-filter');
    const posFilter = document.getElementById('pos-filter');
    const filtersRow = document.getElementById('filters-row');

    let currentStatType = 'individual';
    let currentTable = null;
    let tableData = [];
    let currentSort = { col: null, asc: true };

    async function fetchTables() {
      const res = await fetch(`/tables?type=${currentStatType}`);
      return res.json();
    }

    async function fetchTableData(tableName) {
      const res = await fetch(`/tables/${tableName}`);
      return res.json();
    }

    function populateCategoryDropdown(tables) {
      categorySelect.innerHTML = '<option selected disabled>Select a category</option>';
      tables.forEach(table => {
        const label = table.replace(/_/g, ' ').replace(/^individual |^team /, '');
        const option = document.createElement('option');
        option.value = table;
        option.textContent = label;
        categorySelect.appendChild(option);
      });
    }

    async function updateCategoryList() {
      const tables = await fetchTables();
      populateCategoryDropdown(tables);
    }

    function populateFilters(data) {
      filtersRow.style.display = currentStatType === 'individual' ? 'flex' : 'none';

      const getUniqueValues = (key) =>
        [...new Set(data.map(row => row[key]).filter(val => val != null && val !== ''))].sort();

      const populateDropdown = (selectEl, values) => {
        selectEl.innerHTML = '<option value="">All</option>';
        values.forEach(val => {
          const opt = document.createElement('option');
          opt.value = val;
          opt.textContent = val;
          selectEl.appendChild(opt);
        });
      };

      if (currentStatType === 'individual') {
        populateDropdown(teamFilter, getUniqueValues('Team'));
        populateDropdown(classFilter, getUniqueValues('Cl'));
        populateDropdown(posFilter, getUniqueValues('Position'));
      }
    }

    function renderTableData() {
      let data = tableData;

      if (currentStatType === 'individual') {
        const teamVal = teamFilter.value;
        const classVal = classFilter.value;
        const posVal = posFilter.value;

        data = data.filter(row => {
          return (!teamVal || row['Team'] === teamVal) &&
                 (!classVal || row['Cl'] === classVal) &&
                 (!posVal || row['Position'] === posVal);
        });
      }

      if (!Array.isArray(data) || data.length === 0) {
        dataTable.innerHTML = '<p>No data available</p>';
        return;
      }

      const cols = Object.keys(data[0]);
      let html = '<table class="table table-striped table-bordered"><thead><tr>';
      cols.forEach(col => {
        let arrow = (col === currentSort.col) ? (currentSort.asc ? ' ▲' : ' ▼') : '';
        html += `<th onclick="sortTableByColumn('${col}')">${col}${arrow}</th>`;
      });
      html += '</tr></thead><tbody>';

      data.forEach(row => {
        html += '<tr>';
        cols.forEach(col => {
          html += `<td>${row[col]}</td>`;
        });
        html += '</tr>';
      });

      html += '</tbody></table>';
      dataTable.innerHTML = html;
    }

    function sortTableByColumn(col) {
      if (currentSort.col === col) {
        currentSort.asc = !currentSort.asc;
      } else {
        currentSort.col = col;
        currentSort.asc = true;
      }

      tableData.sort((a, b) => {
        let valA = a[col], valB = b[col];
        const numA = parseFloat(valA), numB = parseFloat(valB);

        if (!isNaN(numA) && !isNaN(numB)) {
          valA = numA;
          valB = numB;
        } else {
          valA = (valA || '').toString().toLowerCase();
          valB = (valB || '').toString().toLowerCase();
        }

        if (valA < valB) return currentSort.asc ? -1 : 1;
        if (valA > valB) return currentSort.asc ? 1 : -1;
        return 0;
      });

      renderTableData();
    }

    statTypeSelect.addEventListener('change', async (e) => {
      currentStatType = e.target.value;
      currentTable = null;
      tableTitle.textContent = '';
      dataTable.innerHTML = '';
      filtersRow.style.display = 'none';
      await updateCategoryList();
    });

    categorySelect.addEventListener('change', async (e) => {
      currentTable = e.target.value;
      tableTitle.textContent = e.target.options[e.target.selectedIndex].text;
      currentSort = { col: null, asc: true };
      const data = await fetchTableData(currentTable);
      tableData = data;
      populateFilters(data);
      renderTableData();
    });

    teamFilter.addEventListener('change', renderTableData);
    classFilter.addEventListener('change', renderTableData);
    posFilter.addEventListener('change', renderTableData);

    async function init() {
      await updateCategoryList();
    }

    init();
  </script>
  <!-- Recruiter Chatbot Widget -->
<style>
  #chatbot-toggle {
    position: fixed;
    bottom: 20px;
    right: 20px;
    background: #0d6efd;
    color: white;
    border: none;
    border-radius: 50%;
    width: 55px;
    height: 55px;
    font-size: 24px;
    z-index: 999;
    box-shadow: 0 0 10px rgba(0,0,0,0.3);
  }

  #chatbot-box {
    position: fixed;
    bottom: 90px;
    right: 20px;
    width: 320px;
    max-height: 480px;
    background: white;
    border-radius: 12px;
    border: 1px solid #ccc;
    box-shadow: 0 8px 20px rgba(0,0,0,0.2);
    display: none;
    flex-direction: column;
    z-index: 999;
  }

  #chatbot-box header {
    background: #0d6efd;
    color: white;
    padding: 10px;
    border-top-left-radius: 12px;
    border-top-right-radius: 12px;
    font-weight: bold;
    text-align: center;
  }

  #chatbot-messages {
    padding: 10px;
    overflow-y: auto;
    flex: 1;
    font-size: 14px;
  }

  #chatbot-input {
    display: flex;
    border-top: 1px solid #ccc;
  }

  #chatbot-input input {
    flex: 1;
    border: none;
    padding: 8px;
    font-size: 14px;
  }

  #chatbot-input button {
    background: #0d6efd;
    border: none;
    color: white;
    padding: 8px 12px;
    cursor: pointer;
  }

  .chatbot-msg.user { text-align: right; margin: 5px 0; }
  .chatbot-msg.bot { text-align: left; margin: 5px 0; color: #333; }
</style>

<button id="chatbot-toggle">💬</button>

<div id="chatbot-box" class="d-flex flex-column">
  <header>Recruiter Assistant</header>
  <div id="chatbot-messages"></div>
  <div id="chatbot-input">
    <input type="text" id="chatbot-query" placeholder="Ask and check if Metric categories pop up..." />
    <button onclick="sendChatQuery()">➤</button>
  </div>
</div>

<script>
  const chatbotToggle = document.getElementById("chatbot-toggle");
  const chatbotBox = document.getElementById("chatbot-box");
  const chatbotMessages = document.getElementById("chatbot-messages");
  const chatbotInput = document.getElementById("chatbot-query");

  chatbotToggle.onclick = () => {
    chatbotBox.style.display = chatbotBox.style.display === "flex" ? "none" : "flex";
  };

  function appendMessage(msg, type) {
    const div = document.createElement("div");
    div.className = `chatbot-msg ${type}`;
    div.textContent = msg;
    chatbotMessages.appendChild(div);
    chatbotMessages.scrollTop = chatbotMessages.scrollHeight;
  }

  async function sendChatQuery() {
    const query = chatbotInput.value.trim();
    if (!query) return;

    appendMessage(query, "user");
    chatbotInput.value = "";

    const statType = document.getElementById("stat-type-select").value;
    const category = document.getElementById("category-select").value;

    try {
      const res = await fetch("/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query, type: statType, category: category })
      });
      const data = await res.json();
      appendMessage(data.response, "bot");
    } catch (e) {
      appendMessage("Error fetching response. Try again.", "bot");
    }
  }

  chatbotInput.addEventListener("keypress", function(e) {
    if (e.key === "Enter") sendChatQuery();
  });
</script>

</body>
</html>


"""
    return render_template_string(html)

# Method to run Flask app; change debug if testing code locally 
def run_api():
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)

Thread(target=run_api, daemon=True).start()
query.awaitTermination()
