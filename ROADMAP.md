# Roadmap

The MVP is what the top of the [README](README.md) promises, for someone who isn't a developer: drop scans
in a folder, and Lindley reads them, sorts the pages into documents and makes searchable PDFs,
and you can ask it about them. All of it from a one-click install.

## Done

- [x] Project scaffolding and CI
- [x] UI design (clickable mockup)
- [x] Database design
- [x] Assembler: grouping Inbox pages into documents
- [x] Intake: hashing, EXIF, splitting PDFs and TIFFs, Tesseract reading
- [x] Folder watcher
- [x] Image checks: blank pages, rotation, handwriting or print
- [x] Duplicates: detection, decisions with undo, API, mockup and the app's screen
- [x] AI connectors, one file each: a local AI, Anthropic, OpenAI, Google, and other OpenAI-compatible services
- [x] Vision model reading for handwriting
- [x] API and the real React UI, built from the mockup
- [x] Searchable PDF export, built from stored readings (no Ghostscript)
- [x] Search: full text, over every page's reading in use
- [x] API keys in Windows Credential Manager
- [x] Settings in the app, built from the mockup
- [x] When each AI may be used: ask first or automatic, with a daily and a monthly limit; every call recorded
- [x] Throttling each AI: calls a minute and at once
- [x] The app tried end to end on real scans, and what broke fixed (AI work in the background, no
  hidden retries, cut-off answers and doubtful AI readings caught)
- [x] Benches for local AI models, with hand-made PDFs matched back to their scans as the answer
  key; typed pages lying upside down now turned, including ones read before
- [x] Mirror images (the back of a carbon copy) found and turned round, including ones read
  before; a person can flip a page too
- [x] Claude (Opus 5.5) tried on real scans: it read 48 hard pages at about 3¢ a page, and sorted
  pages into 9 documents, leaving the groups it was less sure of as suggestions (design/database.md, "Claude on real scans")

## For the MVP

- [x] Ask Lindley (chat with your documents): the AI names the words to search for, Lindley
  finds the pages (the one open first), and the answer cites them; it streams, can be stopped, and
  is kept. Available when the chat job has an AI that can be used; an AI set up but not chosen is
  offered in one click
- [x] Record each sorting call as it's made, not when the whole job ends, so a job cut short
  still shows what it spent
- [x] See where a letter starts without "Dear …", from its date line and who it's to: the rules
  had run a 1940 letter on into the typescript before it (design/database.md, "Confidence bars")
- [x] Show progress while the AI sorts pages: the status bar counts the questions it asks the AI
  ("AI sorting 143 pages · question 2 of 4 so far": answers can raise more), and shows the
  AI sorting on its own too
- [x] Read a page again with Tesseract when a person turns it or flips it left to right in the
  app (or undoes either): before, its text stayed as read the wrong way up or from the mirror
  image. The new reading replaces Tesseract's own, never a person's text or an AI's better
  reading, the Inbox is sorted again with it, and the status bar says so while it's read
- [x] Two AI choices in Setup and Settings, in place of a list of performance tiers: how much AI
  this computer runs, and who does the rest (`providers/tiers.py`). Each job gets the tier's model
  on Lindley's own AI, else the help if it can do the job, else nothing; the choice of AI for each
  job stays underneath, and changing one makes it your own. Every job done on this computer is
  one that isn't sent anywhere or paid for, so the cost of a page falls, and privacy rises, from
  Low to High:
  - **Low** (any computer, even an old one): no AI on this computer. Tesseract and the rules
    read and sort; the help does the rest
  - **Middle** (8 GB of memory): Gemma 4 E2B checks whether each page carries on from the last
    (`lm_continues`), so sorting needs the help less. Reading handwriting, sorting the rest and
    answering questions go to the help. Whether Middle should read handwriting itself needs
    benches on more computers: on a 2019 desktop processor Gemma 4 E4B takes nearly 3 minutes
    a hard page
  - **High** (16 GB, or a graphics card): Gemma 4 E4B reads handwriting, sorts, answers and
    checks pages, all on this computer (6 seconds a hard page on an 8 GB graphics card)
  - **Who does the rest**: nobody (what this computer doesn't do waits for you), your own AI
    server (Ollama, LM Studio or llama-server on another computer), or a cloud AI with a key
- [x] A local AI that comes with Lindley: llama.cpp's `llama-server` (v0.6.0, the Vulkan build),
  started by Lindley when a job first needs it and stopped with it, offline, on this computer
  only (`localai/`). Choosing a tier downloads its model and the engine, once, from Hugging
  Face and GitHub, each file pinned and checked, with progress and Cancel in Settings and the
  status bar. If the graphics can't load a model it runs on the processor alone. Ollama and LM
  Studio stay supported through the local connection. Benched on a 2019 desktop processor and an
  8 GB graphics card (design/database.md, "Lindley's own AI, measured"); this computer's built-in
  graphics have too old a driver to bench
- [x] Whether a page carries straight on from the last, checked by a small local model
  (`lm_continues`, the `continues` job): asked only about pairs the rules may get wrong, its
  "yes" weighed as evidence. On the 23 documents it lifted the groups proposed a little (57% to
  61% rebuilt exactly, none wrong); its "no" split pages that do run on, so it isn't weighed
- [x] A look at the computer at first run (processor, memory, graphics card, free disk) that
  suggests a tier, with how long 100 pages would take (`localai/computer.py`, `tiers.suggest`).
  Setup starts on the highest tier the memory or graphics card can run, a step lower when its
  download wouldn't fit on the disk, and says why. Each tier gives its time for 100 typed and
  100 handwritten pages, from speeds measured on one 2019 desktop: on it, High takes about 6
  minutes for typed pages on the graphics card, and nearly 2 hours on the processor alone,
  most of it sorting (116 s a question, against 2.8 s on the card). Benches on more computers
  wait for the installer
- [x] Installers, built with BeeWare's Briefcase by `.github/workflows/installers.yml` (from the
  Actions tab, or a `v*` tag), each started and quit on a clean machine before it's kept:
  - **Windows:** an MSI (68 MB) with Python 3.13 and Tesseract 5.4.0 in it (only what reading
    needs: `scripts/bundle_tesseract.py`)
  - **Ubuntu 26.04:** a `.deb` (28 MB) on the system's Python 3.14, which brings Tesseract,
    Vulkan and the tray's packages with it
  - **Mac (Apple processors):** an app, unsigned. Tesseract comes from Homebrew (`brew install
    tesseract`)
  - Each starts from the menu without a window: it opens Lindley in the browser, puts its icon
    in the tray, and is one Lindley however often it's started. Quit Lindley is in the app's
    header. Out of the box, scans and PDFs go in Documents/Lindley, and the database in the user
    data folder. Lindley's own AI gets llama.cpp's build for each system and processor, and stops
    with Lindley everywhere (on Ubuntu and the Mac, a process group; one left by a Lindley that
    was killed is ended at the next start). Keys go in the desktop's keyring, with plain words
    when there isn't one
- [x] Reviewed for the MVP (October 2026), and what was found fixed: Settings refuses a database
  place that doesn't hold Lindley's database; Setup starts with Lindley's own Inbox and won't start
  with a folder it can't find; Lindley's own AI works whenever it's needed out of the box (it sends
  nothing and costs nothing); stopping it doesn't wait for a model to load; downloading again
  straight after Cancel works; a model the connection names counts as in use
- [x] Reviewed again (October 11, 2026), code and tests, and what was found fixed: Lindley's own
  AI is asked for afresh once it can't be reached, so a server started again at another port
  (a model downloaded since, graphics that failed, or one that stopped) no longer leaves the
  watcher's calls failing until Lindley restarts; a change of nothing (pages moved to where they
  are, text checked again) has nothing to undo; keeping one of two duplicate documents when one
  is completed says to reopen it, rather than failing; stopping Lindley drops the AI work still
  queued, rather than sending it as Lindley closes; undoing the text a person typed for a page
  the AI hadn't read yet leaves its scan unread again, so the document can't be exported without
  that page's text. Still to decide: whether a document waits to export for its scan or only its
  own pages, and how a scan that failed part way shows the pages it did read (and a page waiting
  for the AI alone shows "reading", not "needs AI")
- [ ] Tried by hand on a clean computer of each kind: install, first-run setup, scans read and
  sorted, a PDF made, a question answered, then uninstall ([design/hand-tests.md](design/hand-tests.md)):
  - [x] Ubuntu 26.04, on a 2019 laptop with 7 GB (October 8, Low tier): installing, Setup,
    reading, turning pages, PDFs, Quit and uninstalling all worked. What it found is below
  - [ ] Ubuntu again, once those are fixed: the same scans, plus search, the tray and Add scans…
  - [ ] Windows and the Mac
- [x] What the Ubuntu test found, using its 14 scans as a test to run again:
  - [x] the rules put a typescript's pages together right, then rated the groups 24–28%, too
    low even for a hint, so nothing showed. Now "Do these go together?" is asked by how sure
    Lindley is that the pages belong together, whole document or not (68% and 65% here), and
    says when pages may be missing (design/database.md, "Together, if not whole")
  - [x] typed page numbers read as "a" or "WwW", beside pencil and red numbers, so pages weren't
    put in order by them. Now the margins are read again on their own, and every typed number
    that shows is read (design/database.md, "Page numbers in the margins"); the pencilled ones
    are for the reading AI
  - [x] a handwritten page whose Tesseract reading is gibberish was called blank and offered to
    be set aside. Now the image check alone says whether a page is blank; ink that was read as
    next to nothing waits, loose in the Inbox, for an AI or a person to read it
  - [x] handwriting with no AI to read it waited only in Review. Now Needs AI lists it too, and
    counts it, saying no AI is set up to read it and how to get one (a person who chose
    Tesseract alone isn't asked)
  - [x] smaller things: the main view showed for a few seconds before Setup did; now only
    Lindley's mark shows ("Lindley is starting…") until it knows whether Setup comes first. The
    example folder path was a Windows one on Ubuntu and the Mac; now the examples, and where to
    copy a path from (File Explorer, Finder or Files), are the system's own. The app's icon was
    a brown page; now it's the app's "L" (`scripts/make_icons.py`, and the favicon)
- [x] The UI, for someone new:
  - [x] the same controls in every view: wherever pages can be changed (the Inbox, Set aside, a
    scan, a document, Review, Needs AI), the toolbar turns, flips and has More…, the page menu
    without right-clicking (`components/PageTools.tsx`); ← → go to the one before or after in
    every view that shows one at a time (`lib/usePageKeys.ts`)
  - [x] zoom and pan on page images: every full scan (Review, a scan, a document's reader and
    side view, Duplicates) is one component, zoomed by its buttons, + − 0, Ctrl+wheel, a double
    click or tap, or a pinch, and panned by scrolling, dragging or a finger
  - [x] next and previous scan, without going back to the Inbox: a scan opened from the Inbox
    or Set aside goes on through the list in the order it's sorted there, and once it's set
    aside or added to a document, the scan that followed it opens
  - [x] say that an exported PDF's text is invisible over the scan, ready to search and select:
    where a document is completed, exported, and in the Export button's tip
  - [x] touch screens, on touchscreen laptops and tablets (not phones: Lindley is a desktop
    app): every control at least 44px when touch is the main input; on any touch screen a
    page's checkbox shows without hovering, a tip shows by pressing and holding, and every
    page menu is on the toolbar's More… as well as a long press. Dragging the toolbar or the
    Ask Lindley window ends cleanly when the browser cancels a touch. Pages, documents and
    folders still drag by mouse only; each has a button or menu that does the same
- [ ] Scans in; originals untouched and a PDF out. Either mode ends with the person's original
  scans, unchanged, and a PDF; copy and move mode differ only in where the originals end up.
  Lindley works on a copy of its own, and lets it go once the document is exported:
  - [x] move mode no longer deletes originals: each is moved, unchanged, into Lindley's keeping
    (the library's `originals`), and only if it's still the file that was read in, so a scan a
    scanner is still writing (a TIFF has no ending to check) stays where it is
  - [x] Complete, then Export, as two steps. Export saves to the Exported folder a person
    chooses in Setup or Settings (a native Save dialog once Lindley is a desktop app): copy
    mode, the PDF; move mode, a folder per document with its PDF and its originals. An original
    with pages in another document too stays with Lindley for now, and the export says so.
    Every completed document can be exported at once
  - [x] after export, Lindley lets go of its working copies and page images, and keeps the
    text, fingerprints, history and where the files went, so search, Ask Lindley and duplicate
    checks still work, and copy-mode originals aren't imported again. Only when the original is
    where Lindley left it, unchanged; else it keeps its copy and says so. Reopening brings the
    copies back from the originals, and says which it needs when one isn't there
  - [x] Reopen asks for originals that aren't where Lindley left them, so a person can show it
    where they are (each checked by its fingerprint); and Add scans… can copy or move: moved,
    Lindley keeps the file as the original and gives it back beside the PDF
  - [ ] the loose ends, each the person's to decide: a scan whose pages are in more than one
    document; Set aside pages Lindley is done with; reopening an exported document (back to
    the Inbox, or add pages to its PDF)
- [ ] The frontend, tested and trimmed:
  - tests of its logic with Vitest (it has none today; lint and the type check are all that
    run): the parser that reads Ask Lindley's answer as it streams (`ask` in `api/client.ts`),
    `useApi` and `invalidate` fetching again after a change, and `withTier`
    (`lib/tiers.ts`), which must set the jobs as the backend's `tiers.py` does
  - screens people rarely open (Settings, Setup) loaded only when they're opened
    (`React.lazy`), then the build's size warning set just above what's left, on purpose. The
    app is one 515 kB file today; served from this computer that costs little, but it keeps
    the first view quick, and matters if Lindley ever runs as a web service

## Security

Lindley has kept private pages private by design (Lindley's own AI and Tesseract send nothing;
a cloud AI only with a key a person adds), but hasn't had a security pass of its own. Before it's
offered to anyone else, look at each of these and decide what to do:

- [ ] Who can reach the app. It has no login: anyone who can reach its port can read and change
  the archive. Today it listens on 127.0.0.1 only, and answers only to its own names (a guard
  against DNS rebinding); but listening on IPv6's own address (`--host ::1`) turns that guard off,
  as only `127.x` and `localhost` are recognised. Listening on a network (`--host 0.0.0.0`)
  needs a login, or a clear warning. A second person signed in to the same computer reaches the
  first person's Lindley on port 8765 (see After the MVP, installing for everyone)
- [ ] Other programs and web pages on the same computer. Changes from another site's pages are
  refused (`FromLindleyOnly`), but requests with no Origin (any program) are allowed, reading is
  open to any page that can get round the browser's same-origin rules, and the dev server's
  origins (`localhost:5173`) are allowed in the installed app too: only in development
- [ ] Headers: stop other sites showing Lindley in a frame (clickjacking: a click on their page
  landing on Lindley's buttons), with `frame-ancestors 'none'` or `X-Frame-Options: DENY`; a
  Content Security Policy; `X-Content-Type-Options: nosniff`
- [ ] Files a person adds or drops in: a size limit on Add scans… uploads, and a look at what a
  hostile PDF or image could do (PDFium and Pillow read them; Pillow already refuses a picture too
  big to open safely). Keep both up to date
- [ ] Dependencies: `pip-audit` and `npm audit` in CI, and Dependabot (or similar) for updates
- [ ] What's sent to a cloud AI, said plainly where it's chosen: page images and text, with
  questions; each company's retention and training terms (Google's calls already ask it not to
  store them)
- [ ] Text on the pages talking to the AI (prompt injection): a scanned page could hold
  instructions for Ask Lindley's AI. No connector gives the AI tools, so it can only mislead an
  answer; keep it that way, or design for it before tools are added
- [ ] Keys and logs: API keys stay in the system's credential store; check no key, and no page
  text sent to an AI, ends up in a log or an error shown in the UI
- [ ] What Lindley downloads: the engine and models are pinned and checked against their SHA-256
  before use; the engine's zip is unpacked by Python's `zipfile`, which keeps files inside the
  folder; review when the catalog changes
- [ ] The archive at rest: the database, scans and PDFs are plain files in the user's folders.
  Whether to offer encryption, or say how to back them up and keep them safe
- [ ] Installers: sign the Windows MSI and the Mac app (the Mac app is unsigned today), so a
  person can tell the download is Lindley's

## After the MVP

- [ ] A new experience for someone new, designed as a whole, with a hint system. Grouping is
  Lindley's most useful feature and nothing points to it (the Ubuntu hand test): hints and
  nudges where they apply (select pages and group them, make folders, move documents into
  them), and a walkthrough the first time
- [ ] A desktop app of its own, rather than a tab in the browser. The browser UI stays as it is,
  the same as a web app's, so Lindley could one day run as a service in the cloud
- [ ] The toolbar floats over the whole app, not only the document pane, and docks to the top,
  bottom or either side. The ideas need working out first
- [ ] Installing on Windows for just yourself or for everyone who uses the computer. The MSI
  already asks (Briefcase builds it to, as `system_installer` isn't set), but only installing for
  one person has been tried. Next:
  - choose it on purpose: keep the question, or set `system_installer` in `pyproject.toml`
  - try both by hand, including uninstalling, and Lindley's own AI and Tesseract from Program
    Files
  - each person signed in gets a Lindley of their own: today every Lindley uses port 8765, so a
    second person who starts it opens the first person's Lindley, and their archive with it
    (Lindley has no login). A port per person, or a check that the Lindley answering is theirs
- [ ] Ask Lindley, next:
  - related pages by meaning (embeddings) alongside the words searched for, once the `embed`
    job fills the `embeddings` table
  - what an answer stopped part-way used (an AI says only at the end)
  - for a cloud AI that handles them well, tools over the read-only views, so it can look
    further itself
  - "Ask Lindley about this page" in the toolbars and right-click menus, as in the mockup
  - a local model benched for answering questions, for High
- [ ] Searchable text for every alphabet: ship a glyphless font, so text outside Windows-1252
  (Greek, Cyrillic, Hebrew and so on) goes into the PDF as it was read
- [ ] Details view: everything Lindley found about a page or document
- [ ] Advanced settings (hidden from standard users): an interface for creating custom connectors. They're files too, built the same way as the built-in ones
- [ ] Settings lists the models each AI offers (each library can list them), so the defaults
  can't go out of date
- [x] AI spending: a monthly limit, and the tokens and estimated cost of each call, shown in
  Settings. Claude's, OpenAI's and Google's calls are priced from their list prices
  (`providers/prices.py`); a local AI's calls count tokens, at no cost
- [ ] AI spending, plainly: what each cloud AI has cost, and what's left, where a person can see it
  - the balance from the provider, where it offers one to an ordinary key: today only OpenRouter
    (`GET /api/v1/key`: the key's limit, what's left, and use by day, week and month). Anthropic
    and OpenAI report spending only to an owner's Admin key, and give no balance; Google gives
    neither
  - Lindley's own estimate everywhere else, from `ai_calls` and `providers/prices.py`, as Settings
    shows today. Every figure says whose it is: the provider's, or Lindley's estimate
  - a link to each provider's billing page (Anthropic's Console, OpenAI's billing, Google AI
    Studio, OpenRouter's credits), as `billing_url` beside each connector's `key_url`, so a
    person can check their balance and spending at the source
  - spending limits in dollars, per connection, a day and a month, beside today's limits on the
    number of calls: set in Settings, and offered when an API key is first added (in Setup and
    in Settings) with a suggested amount. Once one is reached, Lindley stops calling that AI on
    its own, and a call a person asks for says so and asks before going over. Checked against
    Lindley's estimate, or the provider's own figure where it can be read, and saying which
- [ ] The status bar's items open an info card on a click, and close it on another click, Escape
  or a click elsewhere. First, AI spending: the bar shows today's calls and estimated cost; the
  card shows each connection's spending today and this month, its calls on its own against its
  limits, the provider's balance where it can be read, and the link to its billing page
- [ ] Suggest groups of pages Lindley isn't sure of (typescripts, notes) for a person to confirm
- [ ] Local AI models for intake, tuned on real scans, on more computers. Measured so far
  (design/database.md, "Local models" and "Lindley's own AI, measured"): none makes intake
  quicker, and Tesseract stays for typed pages. Next:
  - laptops, and built-in graphics with a current driver, before the first-run look at the
    computer suggests tiers from them
  - EmbeddingGemma for what pages are about, if it helps build documents (no tier downloads it
    until something uses it)
