# AGENTS.md

awp-python is the Python agent SDK for the Agent World Protocol, published on PyPI as `awp-python`. `awp.ClientConnection` is the protocol as a sans-IO state machine, and `awp.aio.AsyncClient` drives it over a WebSocket. It targets one specification revision, which is pinned as the `spec/` submodule and named by `awp.SPEC_REVISION`.

## Checks

```bash
git submodule update --init
uv sync
uv run ruff check && uv run ruff format --check
uv run mypy
uv run pytest --cov
uv run python scripts/sync_spec.py --check
```

CI runs all of these on Python 3.11–3.13. It also runs awp-conformance against `awp-demo` in both time models, and fails unless the claim is AWP-conformant.

## Code

- Protocol behavior belongs in `ClientConnection`. `AsyncClient` adds only transport, timers, and awaiting.
- Cite a requirement ID (`AWP-XXX-NNN`) where the code implements it. Otherwise, comment only what the code can't say.
- Tests run against the scripted world in `tests/world.py`, not against awp-sim.
- `src/awp/_spec/` is copied from `spec/` by `scripts/sync_spec.py`. Never edit it by hand.

## Conformance

`conformance/` holds the reports behind the README's claim, plus the evidence for their `manual` rows. The reports must come from the suite version CI runs. After bumping that pin in `.github/workflows/ci.yml`, regenerate them with the commands in `conformance/README.md`. Then update the versions named there and in the README.

## Commits and pull requests

- Branch from `main` and open a pull request. Merge once CI passes.
- Write the title as one plain sentence in sentence case, with no trailing period, saying what changed: `Run awp-conformance 0.1.0a4 in CI`. When the change is part of a release, end it with the version: `(0.1.0a4)`.
- Add a body only when the title can't carry the reason: one or two short sentences.
- Write commits the way a person on the project would. No `Co-Authored-By` trailers, no "Generated with" lines, and no other mention of AI tools, in commits or in PRs.
- The PR title matches the commit title, and the description is a few lines at most.

## Moving to a new draft revision

1. Check out the revision's tag (`spec-v0.1-draft.N`) in `spec/`.
2. Run `uv run python scripts/sync_spec.py`.
3. Update `SPEC_REVISION` in `src/awp/__init__.py`.
4. Fix whatever the tests report.

## Releasing

1. In a pull request:
   - Set the version with `uv version <version>`, for example `0.1.0a5`.
   - Add a `CHANGELOG.md` entry that starts "Targets specification revision `0.1-draft.N`." and lists the user-visible changes.
2. Once it is merged, tag the merge commit and push the tag:

   ```bash
   git tag -a v0.1.0a5 -m "awp-python 0.1.0a5 (AWP 0.1-draft.N)"
   git push origin v0.1.0a5
   ```

3. `release.yml` checks that the tag matches the version, then builds the package. It publishes through PyPI trusted publishing once someone approves the `pypi` environment. A maintainer gives that approval. Agents never approve deployments, and never publish with a token.

awp-sim depends on awp-python, so release awp-python first when both change.
