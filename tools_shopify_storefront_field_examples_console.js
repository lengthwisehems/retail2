const token = "5b2e1e6299112b3ec028aa43f7fd20c1";
const exampleLimit = 5;
const productSampleSize = 8;
const variantSampleSize = 20;

async function sfQuery(query, variables = {}) {
  const res = await fetch("https://7fam-shop1.myshopify.com/api/unstable/graphql.json", {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Shopify-Storefront-Access-Token": token },
    body: JSON.stringify({ query, variables })
  });
  const json = await res.json();
  if (json.errors) console.warn("GraphQL errors:", json.errors);
  return json;
}

const __cache = new Map();
const unwrap = t => { while (t?.ofType) t = t.ofType; return t || {}; };
const typeLabel = t => t?.kind === "NON_NULL" ? `${typeLabel(t.ofType)}!` : t?.kind === "LIST" ? `[${typeLabel(t.ofType)}]` : (t?.name || t?.kind || "");
const isLeaf = t => ["SCALAR", "ENUM"].includes(unwrap(t).kind);
const topName = s => (String(s).trim().match(/^(\w+)/) || [])[1] || "";
const fmt = v => v == null ? "" : Array.isArray(v) ? v.map(fmt).filter(Boolean).join(", ") : typeof v === "object" ? Object.entries(v).map(([k, x]) => [k, fmt(x)]).filter(([, x]) => x).map(([k, x]) => `${k}: ${x}`).join(" | ") : String(v);
const examples = (rows, key) => [...new Set(rows.map(r => fmt(r?.[key]).trim()).filter(Boolean))].slice(0, exampleLimit).join(", ");
const blocked = errs => new Set((errs || []).flatMap(e => [([...((e.path) || [])].reverse().find(x => typeof x === "string")), (e.message || "").match(/for\s+(\w+)\s+field/i)?.[1]]).filter(Boolean));

async function getFields(typeName) {
  if (__cache.has(typeName)) return __cache.get(typeName);
  const q = `query($t:String!){__type(name:$t){fields{name type{kind name ofType{kind name ofType{kind name ofType{kind name}}}} args{name}}}}`;
  const fields = (await sfQuery(q, { t: typeName }))?.data?.__type?.fields || [];
  __cache.set(typeName, fields);
  return fields;
}

async function buildSelection(typeName, depth = 0, seen = new Set()) {
  if (depth > 2 || seen.has(typeName)) return [];
  seen.add(typeName);
  const fields = await getFields(typeName), out = [];
  for (const f of fields) {
    if ((f.args || []).length) continue;
    if (isLeaf(f.type)) out.push(f.name);
    else {
      const name = unwrap(f.type).name;
      if (!name || ["MetafieldConnection", "MediaConnection", "SellingPlanGroupConnection"].includes(name)) continue;
      const child = await buildSelection(name, depth + 1, new Set(seen));
      if (child.length) out.push(`${f.name} { ${child.join(" ")} }`);
    }
  }
  return out;
}

async function getSamples(typeName) {
  let sel = await buildSelection(typeName);
  if (!sel.length) return [];
  for (let i = 0; i < 5; i += 1) {
    const q = typeName === "Product"
      ? `query($first:Int!){products(first:$first){nodes{${sel.join("\n")}}}}`
      : `query($productFirst:Int!,$variantFirst:Int!){products(first:$productFirst){nodes{variants(first:$variantFirst){nodes{${sel.join("\n")}}}}}}`;
    const vars = typeName === "Product" ? { first: productSampleSize } : { productFirst: productSampleSize, variantFirst: variantSampleSize };
    const r = await sfQuery(q, vars), bad = blocked(r.errors);
    if (!bad.size) return typeName === "Product" ? (r?.data?.products?.nodes || []) : (r?.data?.products?.nodes || []).flatMap(p => p?.variants?.nodes || []);
    const next = sel.filter(s => !bad.has(topName(s)));
    if (next.length === sel.length) break;
    sel = next;
  }
  return [];
}

async function listTypeFields(typeName) {
  const fields = await getFields(typeName);
  const sampleRows = await getSamples(typeName);
  const rows = fields.map(f => ({ field: f.name, type: typeLabel(f.type), examples: examples(sampleRows, f.name) }));
  console.log(typeName, rows);
  console.table(rows);
  return rows;
}

await listTypeFields("Product");
await listTypeFields("ProductVariant");
