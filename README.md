# FinAlly

FinAlly (Finance Ally) is an AI-powered trading workstation: live-streaming market data, a simulated portfolio, and an LLM chat assistant that can analyze positions and execute trades on the user's behalf. Built as the capstone project for an agentic AI coding course, entirely by coding agents.

## Stack

- **Frontend**: Next.js (TypeScript), static export
- **Backend**: FastAPI (Python, managed with `uv`)
- **Database**: SQLite, bind-mounted at `db/`
- **Real-time data**: Server-Sent Events (SSE)
- **AI**: LiteLLM → OpenRouter (Cerebras inference)
- **Deployment**: single Docker container, single port (8000)

## Status

This project is in the planning stage — see [`planning/PLAN.md`](planning/PLAN.md) for the full specification (architecture, API, database schema, and UI design).

## Running it

Once built, the app will run via:

```bash
docker compose up -d --build
```

Then open `http://localhost:8000`. See `planning/PLAN.md` §11 for details.

## License

See [LICENSE](LICENSE).
