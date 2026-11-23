# Name generation: the full pipeline

How the four name fields are actually built, in order, including what has to be settled
before naming can start and which steps have to run more than once.

Ground rules §2 says **what** each name field means and how the segments are laid out.
This document says **how** one gets built. It is written from the Good American scraper,
which is the most developed example, but the structure is the one to follow for any brand.
Where a step is specific to one brand's data it is marked as such.

**The separator varies by brand** (ground rules §2). The examples here use ` | `. Note that
the separator plays **two different roles**, and a port has to parameterise both:

1. the character that **splits the brand's incoming title** into its name part and its
   colour code, and
2. the character that **joins the segments you compose**.

They happen to be the same character in most brands, which hides the distinction until a
brand comes along where they differ.

---

# 0. Before any name can be built

## 0.1 Per-product prerequisites

|#|Step|Why it has to come first|
|-|-|-|
|1|Fetch and filter: the collection feed, plus any search/merchandising feed. Drop products by excluded title terms and excluded product types|An excluded product must never reach naming at all|
|2|Clean the title (see 0.4)|Every later step reads the cleaned text. Cleaning after keyword matching is too late|
|3|Take **everything before the first separator** of the brand's own title|The brand usually ships an already-segmented title (name, then colour code). Naming reads only the part before the first separator|
|4|Read the length keyword, **then** strip the length word (see 0.3)|Order is critical and non-obvious|
|5|Jean Style|Circular with naming; resolved by running the name computation twice (§3.1)|

## 0.2 Per-variant prerequisites

|#|Step|Feeds|
|-|-|-|
|6|Resolve the variant's option slots into a size value and a length attribute|the size segment, the length segment|
|7|Colour value from the first option; **colour code from the second segment of the brand's incoming title**, falling back to the option colour|the colour segment|
|8|Resolve the length segment by priority: **handle -> option attributes -> the title's own length keyword**, then normalise the brand's alternative spelling of "regular" to the canonical one|the length segment|

## 0.3 Read the length keyword before stripping the length word

The length keyword is read from the **raw** title. Every other category is read from the
title **with the length word removed**. Both halves matter:

- **Read it first, or you lose it** - it is about to be deleted.
- **Strip it before the other categories run, or it contaminates them.** A two-letter rise
  keyword matches *inside* a length word and stamps a spurious rise label onto every
  long-inseam style.

The length belongs in its own segment, never in the name.

## 0.4 Light cleaning of the title

The brand's own title is not clean enough to match keywords against. Clean it **once, up
front**, before any keyword table sees it:

|cleaning|rule|
|-|-|
|mojibake|repair double-encoded UTF-8 artifacts first, before anything reads the text|
|quotes and dashes|curly quotes -> straight ASCII; en/em dashes -> space|
|accents|strip them (`KHLOÉ` -> `KHLOE`), so an accented name still matches an unaccented keyword|
|**hyphens**|**replace with a space.** A hyphen is a word separator in brand titles (`SOFT-TECH` is the same thing as `SOFT TECH`). Keyword tables are written with spaces, so an un-normalised hyphen silently misses *every* match|
|**compound fit words**|**collapse the two-word spelling to one.** `Boot Cut` -> `Bootcut`. Brands are inconsistent about these, and the database and the keyword tables use the one-word form|
|invisibles|strip BOM, zero-width spaces and non-breaking spaces|
|whitespace|collapse runs of spaces, strip leading and trailing|

### Scope: clean the title text, not every segment value

Apply this to the **title text only**. Do not run it over the composed output or over the
other segment values - **sizes legitimately contain hyphens**, where the hyphen is a range
rather than a word separator. Good American ships `00-4`, `6-12`, `14-18 PLUS` and
`20-26 PLUS`; a blanket hyphen replacement would turn `00-4` into `00 4` and invent a size
that does not exist.

### Why the compound-word collapse is not optional

It is tempting to handle `Boot Cut` by adding a bare `Boot` keyword instead of collapsing
the spelling. That gets the Jean Style right and corrupts both names, because the second
word is left stranded in the title with nothing to remove it:

```
title:        GOOD CLASSIC BOOT CUT JEANS
Jean Style:   Bootcut          <- correct
Style Name:   GOOD CLASSIC CUT BOOTCUT      <- "CUT" leaked into WHAT'S LEFT
Product:      GOOD CLASSIC BOOTCUT CUT JEANS  <- bare-word expansion doubled it
```

Step 3 (§1) builds WHAT'S LEFT by removing the words of the matched keyword and its label.
A bare `Boot` keyword removes only `BOOT`, so `CUT` survives into the distinctive-words
slot. Collapsing `Boot Cut` -> `Bootcut` in cleaning removes the whole problem one step
earlier, and is the only fix that gets *both* fields right.

---

# 1. The seven-step name computation

One function, called per product. Steps 1-3 gather material; 4-7 assemble it. Keyword
tables are per brand; the shape below is not.

**Step 1 - category label extraction.** Each keyword category is scanned independently in
*maximal-join* mode: collect every keyword that matches, **drop any match that is a
substring of a longer match** in the same category, then emit the surviving labels in
keyword-list order, de-duplicated. Categories whose keywords are short enough to match
inside unrelated words must be matched **whole-word only**.

**Step 2 - adjective special case.** Where one adjective is ambiguous, keep it as-is when
it co-occurs with a particular jean style and re-match it against the rest of the category
otherwise.

**Step 3 - WHAT'S LEFT.** Remove from the title every word that appeared in *any* match -
both the raw matched keywords and the emitted labels. Removal is word-level, not
phrase-level. What survives is the style's distinctive wording: the part no keyword table
knows about.

**Step 4 - the pre-title**, the shared stem of both piped fields. WHAT'S LEFT plus every
category label, concatenated in a **fixed slot order**. Four branches choose the order:

|branch|condition|order|
|-|-|-|
|leading-line|the product line is one of a small group that reads first|product line, then WHAT'S LEFT, then the rest|
|bare|no WHAT'S LEFT **and** no product-line label|pull-on leads|
|no-WHAT'S-LEFT|no WHAT'S LEFT, but a product line exists|product line leads|
|default|everything else|WHAT'S LEFT leads|

Step 4 carries the **full** slot set, including rise, styling, fabric, inseam style, type
and the length keyword.

**Step 5 - the style-name draft.** Same four branches, same relative order, but a
deliberately **smaller slot set**: WHAT'S LEFT, adjective, jean style, product line,
pull-on, and one secondary type. Rise, styling, fabric, inseam style, type and length are
all excluded. This is what makes Style Name shorter than the pre-title:

```
pre-title:   GOOD ICON STRAIGHT DIAMOND JEANS
Style Name:  GOOD ICON STRAIGHT
```

The jean-style slot takes the title's own jean-style label when there is one, and otherwise
falls back to the first word of the resolved Jean Style - which is the circular dependency
(§3.1, §3.3).

**Step 6 - Style Name.** The draft, promoted when it is too thin to stand alone:

|draft state|result|
|-|-|
|empty|styling-or-fabric + the jean style's first word|
|only the jean style, no styling and no fabric|draft + type|
|only the jean style|styling-or-fabric + draft|
|a single word|styling-or-fabric + draft|
|otherwise|the draft unchanged|

Then any brand-specific expansion of an under-specified name, then the shared cleanups
(`90s` casing, accent folding).

**Step 7 - Product Line.** Ordered *contains* rules over the pre-title, first match wins.
Failing that, a derivation from WHAT'S LEFT and the adjective, then a short ladder of
fallbacks, then the product-line label itself.

---

# 2. Assembling the four output fields

Three are composed; one is not.

```
Product            = the brand's own title, lightly rewritten   (NOT derived)
Style Name         = Step 6
Product Title Alt  = pre-title  <sep> colour code  [<sep> length segment]
Variant Title      = Product Title Alt            [<sep> size]
```

The segment contract is **name, colour, length when there is one, size**. The length
segment is **omitted entirely** rather than filled with a default, and the OLD marker
attaches to the length segment, before the size (ground rules §2).

**`Product` is a different kind of field.** It keeps the brand's own wording and is only
*rewritten*: the length word moved to sit immediately before the separator, one length
synonym folded into another, accents and specials stripped, `90s` normalised, abbreviations
expanded. The derived collection word is **not** re-attached here - that happens only in
Style Name and the piped titles.

**A subtlety worth knowing:** `Product` takes the variant's **own length attribute**, while
the piped titles take the **attribute-or-title-keyword** fallback. These are deliberately
different values. A style whose options carry no length must not pick up a spurious
`REGULAR` in `Product` just because the resolved Inseam Label defaults to Regular.

Both piped fields then run a **de-duplication**: when the name itself ends with the length
word *and* the length is also its own segment, the copy inside the name is dropped. The
segment is authoritative. In Variant Title the length sits before the size rather than
last, so every segment after the first is checked, not just the final one.

---

# 3. What repeats, and why

The part most often got wrong when porting.

## 3.1 The name computation runs twice per product

Steps 5 and 6 need Jean Style's first word as a fallback. But Jean Style is read off the
**Step 4 pre-title**, not the raw title - because the pre-title has had the length word
moved out, which keeps multi-word jean-style phrases contiguous instead of split by an
interposed length word.

```
compute_naming(title, jean_style="")        -> pre-title
          v
read Jean Style from the pre-title
          v
compute_naming(title, jean_style=<first word>)  -> the names actually used
```

One extra pass, **not** iteration to a fixed point. The first result is discarded except
for the Jean Style read off it.

## 3.2 The Jean Style fill ladder is interleaved and partly repeated

Each text stage fills rows that then become *donors* for the inheritance pass, so the
inheritance pass is re-run between stages:

```
draft-inheritance -> title+description -> draft-inheritance -> description
  -> draft-inheritance -> handle -> style-name-inheritance -> tags
  -> style-name-inheritance      <- a second time
```

The style-name inheritance runs twice because the tag fallback can supply the first filled
row for a name group, creating a donor that did not exist on the earlier pass.

## 3.3 The Style Name's jean-style slot is filled after the fact

Step 5's fallback is only useful once Jean Style has settled, which is several cross-row
passes later. So the slot is deferred and filled by a dedicated pass that **re-inserts the
word at the correct slot position for whichever Step 4/5 branch the row took**. Carry the
branch name on the row; it exists solely so this late pass can find the right insertion
point instead of appending blindly.

## 3.4 The piped titles are rebuilt from parts, never edited in place

Keep the parts on the row and re-assemble. Every pass that changes a part triggers a
rebuild, and each rebuild re-runs the length de-duplication:

|when|why|
|-|-|
|inline at staging|first construction|
|end of the collection-word pass|that pass rewrites the pre-title|
|the dedicated rebuild pass|a later rule also edits the pre-title|
|multi-inseam pass|**patched**, not rebuilt - adds a length segment, or adopts a sibling's wording|
|OLD marker|**appends only**|

## 3.5 Product Line is recomputed on every rebuild

It reads the pre-title, so any pass that rewrites the pre-title has to recompute it.

## 3.6 Inseam Style is refreshed twice

Once after the size-guide inseam fill, and again after the length/label alignment.

---

# 4. Cross-row passes that change names after staging

None of these can run per-product; they all need the whole corpus staged first.

**Collection-word re-attachment.** A length-specific title drops the collection word its
regular sibling carries. Strip the length word from the name and try each candidate
collection word, keeping one only when it produces a style name that **already exists for
the same colour**. Evidence order: the PDP's sister-variant link first (the only hard
evidence of which collection a length-only title belongs to), then the *exact* colour pool,
with description similarity as a tiebreak when several candidates survive. Compare colours
exactly before any digit-stripped fallback - two numbered washes of one base colour are
genuinely different colourways. When nothing corroborates a collection word, **move the
length word out and guess nothing**. This rewrites both Style Name and the pre-title.

**Single-occurrence expansion rules.** Any rule that fires only when exactly one bare
occurrence exists corpus-wide needs the full row set to know it is solitary.

**Multi-inseam detection and family alignment** (ground rules §3). A style is Multi when
stripping the length segment collides with another style's, counted over **distinct
lengths** rather than distinct styles - one handle carrying two lengths is as much a
multi-inseam style as two handles would be. Anything with a length segment that finds no
partner by name is checked against the PDP's length-switcher family. Then two rewrites: the
unmarked member gains an explicit `REGULAR` segment, and siblings **adopt the regular
member's wording and its Style Name**.

**Colour unification** within a handle-plus-style; most common spelling wins.

**Length alignment to the title** - changes the *label*, not the title: where the title
states a length, a stated length beats a measured one.

**Style Name casing**, applied once after all Style Name mutations.

**The OLD marker**, which appends to `Product` and to both piped fields.

---

# 5. Ordering constraints you cannot violate

1. **Clean the title before any keyword table reads it.** (0.4)
2. **Read the length keyword before stripping the length word.** (0.3)
3. **Jean Style must settle before the Style Name's jean-style slot is filled.** (3.3)
4. **The collection-word pass must precede anything that relies on the piped titles**,
   since it rewrites the pre-title.
5. **The multi-inseam pass must precede the Inseam-Style unification**, because the latter
   groups by the Alt base, which the former rewrites.
6. **The length-alignment pass must be the last step that adds or changes a length segment,
   and must still precede the Inseam-Style unification.** This one has bitten for real:
   placing it at the end of the pipeline with a trailing Inseam-Style refresh silently
   undid the unification and flipped 194 rows to the wrong Inseam Style. It belongs
   immediately after the multi-inseam pass.
7. **The OLD marker must be last among title mutations.** It only ever appends, which is
   what makes it safe there.
8. **Style Name casing last** among Style Name mutations.

And one invariant that makes rule 7 work: **every function that reads a length segment
matches on the segment's leading word, not on equality**, precisely so a segment carrying
the OLD marker still reads as that length. Break this convention anywhere and the OLD
marker breaks everywhere downstream.

---

# 6. Before you call the naming done

- [ ] The title is cleaned once, up front, hyphens and compound fit words included (0.4).
- [ ] No hyphen replacement has touched the sizes; `00-4` is still `00-4`.
- [ ] `00` is still `00`, not `0`.
- [ ] The length segment is **absent**, not `REGULAR`, on single-inseam styles.
- [ ] Every member of a multi-inseam style names its length explicitly.
- [ ] No name contains a length word that is also its own segment.
- [ ] No name contains a stranded half of a compound word (`CUT`, `LEG`, `CROP`).
- [ ] Style Name is shorter than the pre-title, and never carries a colour or a size.
- [ ] Bracketed phrases are intact (ground rules §4).
- [ ] An OLD row still reports the right Inseam Label and Inseam Style.
- [ ] Spot-check the output against the correction file you were given, and ask about any
      field that disagrees rather than assuming (ground rules §0).
