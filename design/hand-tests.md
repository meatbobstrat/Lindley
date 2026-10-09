# Trying Lindley by hand

The MVP's last step is trying each installer on a clean computer, as someone who isn't a
developer would use it. CI only starts and quits them. This page records how a hand test is
done, what each run found, and what's left to fix.

## How a run goes

1. **Build.** Run the [Installers workflow](../.github/workflows/installers.yml) on `main` and
   download the artifact for the system being tried (`gh run download <id> -n lindley-ubuntu`).
2. **A clean computer.** Lindley, its settings, its data and Tesseract have never been on it
   (on Ubuntu: `which tesseract` prints nothing; `~/.config/Lindley`, `~/.local/share/Lindley`
   and `~/Documents/Lindley` don't exist). Note its processor, memory, graphics and free disk.
3. **Install,** and check that Tesseract came with it (`tesseract --list-langs`: `eng`, `osd`) and
   that Lindley is in the menu.
4. **First start** from the menu: the browser opens on Setup, and Lindley's icon is in the
   tray. Starting it again opens the same Lindley.
5. **Setup.** Note what it says about the computer and the tier it suggests. Choose the tier and
   help the run is for, and check the settings file it writes.
6. **Scans in,** some through the watched Inbox folder and some through Add scans….
7. **What Lindley made of them,** against the batch's answer key: documents, hints, Needs AI,
   Needs your review, pages turned upright, duplicates.
8. **Sort by hand:** accept a suggestion, make a document, move and reorder pages, rename, Undo.
9. **Search** for words you know are on a page, and a word you corrected.
10. **Export a PDF** and open it in the system's viewer: pages upright, Ctrl+F finds words, and
    selecting text lands on them.
11. **Ask Lindley** a question.
12. **Quit** from the header and from the tray: no Lindley or `llama-server` left running. Start
    again: no Setup, and the work is still there.
13. **Uninstall.** The menu entry goes. The library, the database, the settings and the logs stay,
    as a person's data should (on Ubuntu, a package never touches the home folder).

## The test scans

`test_scans/aa_demo_scans/` holds 452 scans (JPEGs) for development and testing. It's
gitignored, as all scans are: the repo is public. The first runs use 14 of them, whose answer
key Claude read from the scans:

| Document | Scans, in order | Page number | What it tests |
| --- | --- | --- | --- |
| A. A typescript about the Tonopah epidemic | Image (2) to (6) | 7 typed; 8, 10, 11, 12 in pencil | (2) is upside down. Page 9 is missing. (6) ends the piece. Archive numbers 48–53 beside them, in red and circled in pencil |
| B. A memoir of Leigh Hunt, "By Lindley C. Branson" | Image (7), (8), (9), (10), (12), (13) | 1, 2, 4, 5, 7, 8 | (12) and (13) are upside down. (9)'s number doesn't show. Pages 3 and 6 are missing |
| C. A dog story | Image (64) | none | Lying sideways, with typed-over words and pencil corrections |
| D. A handwritten letter (1902) | Image (96), (97) | 1, 2 | Copperplate handwriting, some of it sideways in the margin |

A good result: A and B each one document (or split where pages are missing, with an "Add to
…?" hint), never joined to each other; C on its own; D waiting for a person or an AI to read it.

## Ubuntu 26.04, October 8, 2026

A laptop with a fresh Ubuntu 26.04: Intel Core i5-1035G1 (4 cores, 2019), 7 GB of memory, Intel
Iris Plus graphics, no graphics card. The `.deb` from `main` (5b58700). Low tier, nobody helping,
scans copied. The 14 scans above.

### What worked

- **Install.** apt brought Tesseract 5.5.0 with English and the orientation check. Lindley was in
  the menu.
- **Starting.** From the menu it opened the browser on Setup. A second start opened the same
  Lindley.
- **Setup.** It read the computer correctly ("7 GB of memory, so it can run Low. Middle needs 8
  GB") and suggested Low. It saved settings in `~/.config/Lindley`, the database and logs in
  `~/.local/share/Lindley`, and the Inbox and library in `~/Documents/Lindley`.
- **Reading.** All 14 scans were read: typed pages at 81–89%, the handwritten ones at 25–40%,
  which put both in Needs your review. The three upside-down pages and the sideways one were
  turned upright. A page turned in the app was read again, and its new text showed in a few seconds.
- **PDF.** A document made by hand exported as a PDF with each scan upright and its text laid
  invisibly over it (Helvetica, text render mode 3).
- **Ask Lindley** with no AI said plainly what it can do, and answered.
- **Quit and uninstall.** Quit from the header left nothing running. `apt remove` took Lindley
  out of the menu and left the person's data.

### What went wrong

1. **The rules sorted the pages right, then hid it.** `segment` made exactly the answer key's
   groups (A from (2) to (6), B from (7) to (13)) but rated them 28% and 24% sure. That's under
   `hint_at` (45), so the Inbox showed 14 loose scans and no hint at all. Reproduced on Windows
   with `scripts/intake.py --no-ai`. With no fitted `GROUP_WEIGHTS`, `segment._confidence` takes
   the weakest link inside a group (0.68 here) and cuts it three times for a typescript: no
   start of a document (×0.8), no end (×0.85), and no kind of document recognised (×0.6). That
   leaves 41% of a group whose pages plainly run on from one to the next.
2. **No typed page number was read.** Every typed page has one, but Tesseract read them as
   "a", "WwW", "= = �" and the like, perhaps because pencil and red numbers are written beside
   them. So nothing was put in order by its page number, and nothing said "Page 9 seems to be missing".
3. **A handwritten page was called blank.** Tesseract's reading of the letter is gibberish, so
   the rules took the page for a blank one: "Lindley thinks 2 scans aren't part of any document.
   It looks blank. Set them aside". The image check hadn't found the page blank, and it marked
   it handwritten.
4. **Handwriting with no AI goes nowhere but Review.** Needs AI showed 0 although both letter
   pages need an AI to be read. Decide whether they belong in Needs AI too, saying no AI is set
   up and how to get one.
5. **Nothing says grouping is there.** With no documents and no hints, nothing told the person
   that they can select pages and group them, make folders, and move documents into them.
   Grouping is Lindley's most useful feature, and here it couldn't be seen.
6. **Smaller things:**
   - The main view shows for a few seconds before Setup appears.
   - The example folder path is a Windows one (`E:\Scans`) on Ubuntu and the Mac.
   - The app's icon (a brown page) isn't the "L" mark the app shows.
   - Nothing says the PDF's text is invisible over the scan, so it looks like there is none
     until you search it.

### The person's notes on the UI

- Controls differ from view to view: turning a page, for one, is in the toolbar in some views
  and not others.
- The page images need zoom and pan.
- A document view needs next and previous: today it's back to the Inbox for each one.
- Grouping needs hints and nudges where they apply, and a walkthrough for someone new.
- The toolbar should float over the whole app, not just the document pane, and dock to the top,
  bottom or either side. There are ideas still to work out first.

### Not tried yet

Search, the tray icon and its menu, adding scans with Add scans…, and Ctrl+F and selecting text
in the exported PDF in Ubuntu's own viewer (the text is in the file).

### Next

Fix 1 to 3 and make grouping visible (5), using the 14 scans as a test that can be run again
here (`scripts/intake.py --no-ai` on the 14 in a scratch library).

- **1 is fixed.** "Do these go together?" is now asked by how sure Lindley is that the pages
  belong together, whole document or not (the weakest link inside), not by how sure it is that
  they're the whole document. A and B are both asked about, at 68% and 65%, with exactly the
  answer key's pages, and each says "Nothing marks where it starts or ends, so it may be part of
  a longer document". See design/database.md, "Together, if not whole".
- **2 is fixed, as far as Tesseract can go.** Not every page has a typed number: A's 8, 10, 11
  and 12 are pencilled by hand, and only its 7 is typed. The margins are now read again on their
  own, and every typed number that shows is read, sure: A's 7, and B's 2, 7 and 8. B now says
  "Page numbers 2–8 run in order" and "Page numbers run 7 → 8". B's 5 has faded to look like a
  2, so it's left unread, and the pencilled ones are for the reading AI. See design/database.md,
  "Page numbers in the margins".

Then install again on the same laptop, and try the same scans plus the steps not tried
yet. Then Windows (the build failed on Chocolatey being down, a 503, and needs running again)
and the Mac. After that, Middle or High, which downloads models and runs `llama-server` on Linux
for the first time.
