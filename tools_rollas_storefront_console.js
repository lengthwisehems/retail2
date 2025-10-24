(function() {
  const endpoint = 'https://rollas-us.myshopify.com/api/2023-01/graphql.json';
  const accessToken = 'b14395a79f8e853baf0fad52c71553c3';
  const pageSize = 100;

  const query = `
    query WomensJeans($cursor: String, $pageSize: Int!) {
      products(
        first: $pageSize
        after: $cursor
        query: "collection:womens AND tag:'category:Jeans'"
      ) {
        edges {
          cursor
          node {
            id
            handle
            title
            description
            descriptionHtml
            vendor
            productType
            tags
            createdAt
            updatedAt
            publishedAt
            onlineStoreUrl
            totalInventory
            featuredImage { url altText }
            options { id name values }
            collections(first: 20) {
              edges { node { id handle title } }
            }
            media(first: 20) {
              edges {
                node {
                  __typename
                  ... on MediaImage { image { url altText } }
                  ... on Video { sources { mimeType url } }
                  ... on Model3d { sources { mimeType url } }
                  ... on ExternalVideo { host embedUrl }
                }
              }
            }
            variants(first: 250) {
              edges {
                node {
                  id
                  sku
                  title
                  availableForSale
                  currentlyNotInStock
                  quantityAvailable
                  requiresShipping
                  barcode
                  weight
                  weightUnit
                  selectedOptions { name value }
                  priceV2 { amount currencyCode }
                  compareAtPriceV2 { amount currencyCode }
                  unitPriceMeasurement {
                    measuredType
                    quantityUnit
                    quantityValue
                    referenceUnit
                    referenceValue
                  }
                  image { url altText }
                }
              }
            }
          }
        }
        pageInfo { hasNextPage endCursor }
      }
    }
  `;

  const unwrapEdges = (connection, transform = node => node) =>
    (connection?.edges || []).map(({ node }) => transform(node));

  const normalizeVariant = node => {
    const { priceV2, compareAtPriceV2, selectedOptions, ...rest } = node;
    const optionMap = {};
    (selectedOptions || []).forEach(opt => {
      if (opt?.name) optionMap[opt.name] = opt.value ?? null;
    });
    return {
      ...rest,
      price: priceV2?.amount ?? null,
      priceCurrencyCode: priceV2?.currencyCode ?? null,
      compareAtPrice: compareAtPriceV2?.amount ?? null,
      compareAtPriceCurrencyCode: compareAtPriceV2?.currencyCode ?? null,
      selectedOptionMap: optionMap,
    };
  };

  const normalizeProduct = node => ({
    id: node.id,
    handle: node.handle,
    title: node.title,
    description: node.description,
    descriptionHtml: node.descriptionHtml,
    vendor: node.vendor,
    productType: node.productType,
    tags: node.tags,
    createdAt: node.createdAt,
    updatedAt: node.updatedAt,
    publishedAt: node.publishedAt,
    onlineStoreUrl: node.onlineStoreUrl,
    totalInventory: node.totalInventory,
    featuredImage: node.featuredImage,
    options: node.options,
    collections: unwrapEdges(node.collections, c => ({
      id: c.id,
      handle: c.handle,
      title: c.title,
    })),
    media: unwrapEdges(node.media, m => ({
      __typename: m.__typename,
      image: m.image ?? null,
      sources: m.sources ?? null,
      host: m.host ?? null,
      embedUrl: m.embedUrl ?? null,
    })),
    variants: unwrapEdges(node.variants, normalizeVariant),
  });

  const fetchPage = async cursor => {
    const response = await fetch(endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'X-Shopify-Storefront-Access-Token': accessToken,
      },
      body: JSON.stringify({ query, variables: { cursor, pageSize } }),
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status} – ${response.statusText}`);
    }

    const payload = await response.json();
    if (payload.errors?.length) {
      throw new Error(JSON.stringify(payload.errors, null, 2));
    }

    return payload.data.products;
  };

  const run = async () => {
    const products = [];
    let cursor = null;
    let page = 0;

    do {
      page += 1;
      const connection = await fetchPage(cursor);
      const batch = unwrapEdges(connection, normalizeProduct);
      products.push(...batch);
      console.log(`Fetched page ${page}: ${batch.length} products`);
      cursor = connection.pageInfo?.hasNextPage ? connection.pageInfo.endCursor : null;
    } while (cursor);

    const variantRows = products.flatMap(product =>
      product.variants.map(variant => ({
        productHandle: product.handle,
        productTitle: product.title,
        sku: variant.sku,
        variantTitle: variant.title,
        availableForSale: variant.availableForSale,
        quantityAvailable: variant.quantityAvailable,
        price: variant.price,
        compareAtPrice: variant.compareAtPrice,
      })),
    );

    const productRows = products.map(product => ({
      handle: product.handle,
      title: product.title,
      productType: product.productType,
      vendor: product.vendor,
      totalInventory: product.totalInventory,
      variantCount: product.variants.length,
      tags: product.tags.join(', '),
    }));

    const payload = {
      fetchedAt: new Date().toISOString(),
      productCount: products.length,
      variantCount: variantRows.length,
      products,
    };

    const prettyJSON = JSON.stringify(payload, null, 2);

    console.log(`\nRetrieved ${products.length} products and ${variantRows.length} variants.`);
    console.table(productRows);
    console.table(variantRows);

    window.rollasJeansData = payload;
    window.rollasJeansJSON = prettyJSON;

    if (typeof copy === 'function') {
      copy(prettyJSON);
      console.log('Pretty JSON copied to clipboard.');
    } else {
      console.log('Pretty JSON output:\n', prettyJSON);
    }
  };

  run().catch(err => {
    console.error('Storefront query failed:', err);
  });
})();
