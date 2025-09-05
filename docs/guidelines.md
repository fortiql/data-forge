# 📜 Documentation Guidelines – Data Forge

Data Forge is a **modern data stack playground**.  
It contains Airflow, Trino, Spark, Kafka, Debezium, Hive Metastore, MinIO, ClickHouse, Superset, JupyterLab, and supporting services.  
Our documentation must be as **reliable as the stack itself**: minimal, pragmatic, and clear. Think of Marcus Aurelius writing instructions for engineers.

---

## 🎯 Goals

- **Clearness** → Write for your future self and teammates, not to impress.  
- **Pragmatism** → Every line must help someone run, debug, or extend the stack.  
- **Consistency** → Same style across services and profiles (`core`, `airflow`, `explore`).  

---

## ✍️ Tone

- **Stoic, concise, calm.** Avoid filler and hype.  
- **Explain, don’t sell.** Assume the reader is smart but tired.  
- **Fact → Context.** Present command or config, then explain why.  

Example:  
> ❌ “Kafka is the backbone of streaming at internet scale!”  
> ✅ “Kafka provides the message bus for Debezium CDC events into Spark and ClickHouse.”  

---

## 💡 Structure of Every Doc

1. **Title** → short, factual.  
2. **Why** → one sentence explaining why the service/module matters.  
3. **How** → instructions, configs, `docker compose` profiles.  
4. **Notes** → gotchas, caveats, or links to deeper docs.  

---

## 🤖 Copilot & AI Usage

- Treat Copilot like a **junior teammate**: fast at scaffolding, weak at decisions.  
- Always **simplify and re-explain** AI-generated blocks.  
- If raw Copilot snippets are kept, mark them with 🪶.  

---

## 🎨 Styling & Emojis

Emojis are **semantic markers**, not decoration:

- 📜 = document / guideline  
- ⚙️ = config / setup  
- 🚀 = action / run command  
- 🧩 = component / service  
- 🛑 = warning / caveat  
- ✅ = best practice / correct usage  
- ❌ = anti-pattern / wrong usage  
- 🪶 = Copilot/AI suggestion  

Keep them in **headers and call-outs**, not every line.  

---

## 📏 Examples

**Good:**  
```markdown
## 🚀 Running Airflow (profile: airflow)

To start all Airflow services:  
docker compose --profile airflow up -d

🛑 Note: requires `postgres` and `redis` profiles enabled and healthy.
```

**Bad:**  
```markdown
## Airflow is awesome!!! 😍😍😍

Just run docker-compose up and it should work lol
```

---

## 🧩 Service Docs

Each service doc (e.g., `docs/airflow.md`, `docs/trino.md`) must contain:

- **Profile** → which profile it belongs to (`core`, `airflow`, `explore`).  
- **Dependencies** → which services must be healthy first (`postgres`, `redis`, etc.).  
- **Usage** → key commands or endpoints.  
- **Caveats** → resource requirements, configs to watch.  

---

## 🪶 On Clearness

- If it can’t be read in one breath, split it.  
- If adjectives don’t change meaning, cut them.  
- If Copilot generates verbosity, compress to **core utility**.  

---

## 🪨 Stoic Reminder

> “Don’t write to shine. Write to serve.  
> Don’t add what can be removed.  
> Don’t omit what is necessary.  
> This is enough.”  
