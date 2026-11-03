/*
 * Substituto do SDK da VisitorAPI — mesma assinatura:
 *   VisitorAPI("qualquer-id", data => {...}, err => {...})   // callback
 *   VisitorAPI("qualquer-id").then(data => {...})            // promise
 *
 * Também empurra os dados pro dataLayer (GTM) no evento "visitor_data".
 * Troque ENDPOINT pela URL do seu Worker.
 */
(function (w) {
  var ENDPOINT = "https://visitor.ads-lsantanaf.workers.dev/";
  var CACHE_KEY = "visitor_data_v1";

  function pushDataLayer(d) {
    w.dataLayer = w.dataLayer || [];
    var flat = { event: "visitor_data" };
    for (var k in d) flat["visitor_" + k] = Array.isArray(d[k]) ? d[k].join(",") : d[k];
    w.dataLayer.push(flat);
  }

  function fetchData() {
    try {
      var c = sessionStorage.getItem(CACHE_KEY);
      if (c) return Promise.resolve(JSON.parse(c));
    } catch (e) {}
    return fetch(ENDPOINT, { cache: "no-store" })
      .then(function (r) { return r.json(); })
      .then(function (j) {
        if (j.status !== 200) throw new Error(j.description || "erro");
        try { sessionStorage.setItem(CACHE_KEY, JSON.stringify(j.result)); } catch (e) {}
        return j.result;
      });
  }

  var pending = null;
  w.VisitorAPI = function (_projectId, onSuccess, onError) {
    pending = pending || fetchData().then(function (d) { pushDataLayer(d); return d; });
    if (onSuccess || onError) {
      pending.then(function (d) { onSuccess && onSuccess(d); })
             .catch(function (e) { onError && onError(500, e.message); });
    }
    return pending;
  };
})(window);
