# Firestore Writer with TTL (Time To Live) Tag

**Firestore Writer with TTL** Tag makes it possible to write data from [**Server-side Google Tag Manager**](https://developers.google.com/tag-platform/tag-manager/server-side/overview) to [**Firestore**](https://cloud.google.com/products/firestore).

The unique function with this Tag is that it allows you to write [**TTL (Time To Live)**](https://firebase.google.com/docs/firestore/ttl) directly to Firestore from Server-side GTM.

## Functionality

* **Add Event Data**: Send all sGTM event data to the Firestore document
* **Merge document keys**: Merge keys into Firestore documents
* **Add Timestamp**: Adds timestamp in milliseconds
* **Add Time to Live**: Adds TTL (Time To  Live) Date and Time

<img src="images/firestore-writer-with-ttl-sgtm-tag-template.png" alt="Firestore Writer with Time To Live SGTM Tag Template" />

The functionality and look of the Tag Template is somewhat similar to the [**Firestore Writer Tag**](https://github.com/stape-io/firestore-writer-tag).

However, this Tag Template integrates with the Firestore API in a different way, and calculates Date and Time needed for TTL. This makes it possible to write TTL to Firestore directly from Server-side GTM.

