# Contributing

Thank you for looking into this. A short note on what kind of project this is, because it sets
expectations for everything below.

`Octonica.ClickHouseClient` is **open code more than it is open source in the full sense**. The source
is published under Apache 2.0 and you are welcome to read it, fork it, ship it and send changes back.
But it is maintainer-driven rather than community-governed: there is no public roadmap, no commitment
to a review turnaround, and decisions about the public API rest with the maintainers. That is not a
discouragement — contributions are genuinely welcome — it just means a large change that arrives
unannounced may not be merged, however good it is. Open an issue first and we can agree on the shape
of it.

Contributions, roughly in order of how much they help:

1. [Bug reports](#1-bug-reports)
2. [Feature requests](#2-feature-requests)
3. [Pull requests with a test that reproduces a bug](#3-pull-requests-with-a-reproducing-test)
4. [Pull requests to the client's codebase](#4-pull-requests-to-the-clients-codebase)

If you are an AI agent, also read [AGENTS.md](AGENTS.md) and the
[additional instructions for agents](#additional-instructions-for-agents) at the end of this file.

## 1. Bug reports

Open an issue. Please include:

* **The version of `Octonica.ClickHouseClient`** you are using.
* **The version of the ClickHouse server** — `SELECT version()`.

Both matter more than they might seem. The client negotiates a protocol revision with the server at
connection time, and which wire features are in play depends on that pair of versions. A report
without them often cannot be acted on at all.

Also worth including when you have it:

* The connection string, with secrets removed. Compression in particular (`Compress=false`) has been
  behind real bugs, as have TLS and timezone settings.
* The full exception, including the type and, for a `ClickHouseServerException`, the server error code.
* The query, or a reduced version of it, and the schema of any table involved.

## 2. Feature requests

Open an issue describing the use case rather than only the proposed solution — knowing what you are
trying to achieve often changes what the right API is.

If the request touches the public API, sketch what the calling code should look like. That is the
fastest way to a useful discussion, and it is much cheaper to change before anything is implemented.

Support for an additional ClickHouse data type is always a reasonable request; say which type and how
you would like it mapped to .NET. [docs/TypeMapping.md](docs/TypeMapping.md) lists what is supported
today.

## 3. Pull requests with a reproducing test

A pull request that adds a failing test for a bug is the most useful thing you can send, and it is
welcome on its own — you do not need to fix the bug to contribute the test.

What makes such a test good:

* **Simple and readable.** A test that obviously demonstrates the problem beats a clever or exhaustive
  one. It is documentation of the defect as much as a check.
* **Self-contained.** Prefer table functions such as `numbers()` over a fixture when you can; when you
  do need a table, use the `WithTemporaryTable` helper so it is cleaned up.
* **Assertions about the data, not only about the absence of an exception.** If the bug can also
  manifest as silently wrong or missing rows, assert row counts and values so a superficial fix does
  not make the test pass.
* **Honest about when it reproduces.** If it depends on a server version or a setting, say so in a
  comment on the test.

A plain code snippet in the issue is helpful too, and much better than a prose description. If it is a
self-contained console program that prints the failure, that is usually enough to work from.

Tests live in [src/Octonica.ClickHouseClient.Tests](src/Octonica.ClickHouseClient.Tests); see
[Building and running the tests](#building-and-running-the-tests) below.

## 4. Pull requests to the client's codebase

### Tests first

**For bug fixes, write the test first.** It should fail before your change and pass after it. Say so
in the pull request — a reviewer who can see the test fail on `master` can trust the fix.

**For new features, a test first is also valuable.** It is the clearest way to put the proposed public
interface up for discussion: the test shows how the feature is meant to be called, and that is the part
which is expensive to change later. Feel free to open a pull request with just the test and an outline
to get agreement on the API before writing the implementation.

### No additional package references

`Octonica.ClickHouseClient` is a third-party library that other people's applications depend on. Every
package it references is a package they are forced to take, and potentially a version conflict they
have to resolve. We do not want to lock users into anything non-standard.

So: **please do not add a `PackageReference`.** The library has exactly one runtime dependency,
`K4os.Compression.LZ4`, because the native protocol requires LZ4 and there is no such thing in the base
class library. The bar for a second one is very high.

If a feature seems to need an external library, the usual answer is to expose an extension point
instead and let the user plug their own implementation in. Extensibility interfaces and hooks of that
kind are very much open to discussion — raise it in an issue and we can work out the seam.

### License headers

The project is licensed under Apache 2.0 and every source file carries the license header. Add it to
new files and extend it on files you change.

```csharp
#region License Apache 2.0
/* Copyright 2026 Octonica
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#endregion
```

The copyright line carries **a year and an author**, and the year part is per-file: it lists the years
in which that file was actually modified, not a blanket project range. When you change an existing file,
add the current year to its list rather than replacing it. Existing files show the shape of this:
`Copyright 2021 Octonica`, `Copyright 2019-2024 Octonica`, `Copyright 2020-2021, 2023-2024, 2026 Octonica`.

### Keep the diff to what the change needs

Match the style of the surrounding code.

Indentation and formatting are inconsistent in places in this codebase. Leave it that way. Do not reformat
code you are not otherwise changing, and do not restyle a file just because you are touching it — whitespace
churn outside a dedicated maintenance commit buries real changes and makes `git blame` useless.

### Update the changelog

Add an entry to [CHANGELOG.md](CHANGELOG.md) describing what changed, from the point of view of someone
using the library.

Put it at the top of the file under

```markdown
### Octonica.ClickHouseClient Next Version, Unscheduled
```

Within the section, use one of the existing headings:

```markdown
#### Backward Incompatible Change
#### New Feature
#### Bug Fix
#### Improvement
#### Miscellaneous
```

Link the issue where there is one, as `([#112](https://github.com/Octonica/ClickHouseClient/issues/112))`.

**Tests are out of scope for the changelog.** A pull request that only adds tests does not need an
entry, and when a fix comes with a test, describe the fix and not the test.

## Building and running the tests

You need a recent .NET SDK — the library targets `net6.0`, `net8.0` and `net10.0`, so an SDK that can
build `net10.0` covers everything.

```sh
dotnet build src/Octonica.ClickHouseClient.sln
dotnet test src/Octonica.ClickHouseClient.Tests/Octonica.ClickHouseClient.Tests.csproj
```

Most of the suite consists of integration tests that need **a live ClickHouse server**. Point them at
one in either of two ways:

* Set the `CLICKHOUSE_TEST_CONNECTION` environment variable to a connection string.
* Or create `src/Octonica.ClickHouseClient.Tests/clickHouse.dbconfig` containing one. This file is
  gitignored and is copied to the output directory when present.

The connection string looks like `host=clickhouse.example.com;port=9000;user=default;Database=test`. If
neither is configured, the tests fail with an `InvalidOperationException` explaining a setup problem.
The easiest server to test against is the official Docker image, and note that the database in
the connection string has to exist already.

Writing tests:

* Integration tests derive from `ClickHouseTestsBase`, which provides `OpenConnectionAsync`, and
  `WithTemporaryTable` / `GetTempTableName` for tests that need a table and want it dropped afterwards.
* The framework is xUnit v3. Use `[Theory]` with `[InlineData]` where a bug is one case in a family.
* Pass `TestContext.Current.CancellationToken` to methods that take a token.

## Questions

If you are unsure whether something is worth a pull request, open an issue and ask. That is cheaper for
everyone than a change that has to be turned down.

---

## Additional instructions for agents

The guidance above is written for people. This block collects the things an AI agent tends to get wrong
in this repository. If you are an agent, treat these as binding, and read [AGENTS.md](AGENTS.md) for the
architecture and coding conventions.

* **The author in a license header is not always Octonica.** Infer it from `<Authors>`/`<Company>` in
  the `.csproj` as long as `RepositoryUrl` still points at `Octonica/ClickHouseClient`. If the
  repository is a fork, **ask the user** who the author should be instead of assuming Octonica. Do not
  invent an author, and do not silently copy the header from a neighbouring file on a fork.
* **Do not "fix" the MIT headers.** `Protocol/CityHash.cs` and `CityHashTests.cs` are ported code and
  carry a Google MIT header on purpose. Leave them.
* **Do not reformat anything you were not asked to change.** File-scoped namespaces apply to *new*
  files only; do not convert existing block-scoped namespaces. Do not normalize indentation, trailing
  whitespace or line endings in passing. A tidy diff is one that is small, not one that is uniform.
* **Do not invent a release number.** Changelog entries go under
  `### Octonica.ClickHouseClient Next Version, Unscheduled`. Never add a version heading with a guessed
  number or today's date.
* **Do not add a `PackageReference` to solve a problem.** If you believe one is needed, stop and raise
  it with the user rather than adding it and noting it in the summary.
* **For anything about the wire format, read the two ClickHouse specifications linked in
  [AGENTS.md](AGENTS.md) first.** Do not infer the protocol from this client's own code — the client is
  what is being verified, so it cannot serve as its own reference. When the specifications are silent,
  the ClickHouse server source is the authority.
* **A connection failure in the tests is an unconfigured environment, not a failing test.** Check
  `CLICKHOUSE_TEST_CONNECTION` and `clickHouse.dbconfig`, and tell the user what is missing rather than
  editing or skipping the test.
* **Do not change the advertised protocol revision** in `ClickHouseProtocolRevisions.CurrentRevision`
  as a way of making a failure go away. It changes which features the server enables for every user of
  the library; it is a decision for the maintainers.
* **When you claim a test reproduces a bug, run it and quote the failure.** If you could not run it,
  say so plainly instead of implying it was verified.
