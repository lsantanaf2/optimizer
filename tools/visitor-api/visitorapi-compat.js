/*
 * Visitor API propria — modo COMPATIVEL com o template oficial da visitorapi.com.
 *
 * Empurra pro dataLayer exatamente o mesmo evento e as mesmas chaves que o
 * template "VisitorAPI" (gallery: visitor-api-google-tag-manager) empurrava:
 *   event: "visitor-api-success" + visitorApi<Campo>
 *   event: "visitor-api-error"   + visitorApiErrorCode / visitorAPIErrorMessage
 *
 * Com isso o conteiner nao precisa de NENHUM ajuste: acionadores, variaveis DLV
 * e as tags de GA4/Meta que consomem esses dados continuam funcionando iguais.
 *
 * Mantem tambem a assinatura VisitorAPI(id, onSuccess, onError) / .then().
 */
(function (w) {
  var ENDPOINT = "https://visitor.ads-lsantanaf.workers.dev/";
  var CACHE_KEY = "visitor_data_v1";

  function push(o) {
    w.dataLayer = w.dataLayer || [];
    w.dataLayer.push(o);
  }

  function pushSuccess(d) {
    push({
      event: "visitor-api-success",
      visitorApiIpAddress: d.ipAddress,
      visitorApiCountryCode: d.countryCode,
      visitorApiCountryName: d.countryName,
      visitorApiCurrencies: d.currencies,
      visitorApiLanguages: d.languages,
      visitorApiRegion: d.region,
      visitorApiCity: d.city,
      visitorApiCityLatLong: d.cityLatLong,
      visitorApiBrowser: d.browser,
      visitorApiBrowserVersion: d.browserVersion,
      visitorApiDeviceBrand: d.deviceBrand,
      visitorApiDeviceModel: d.deviceModel,
      visitorApiDeviceFamily: d.deviceFamily,
      visitorApiOs: d.os,
      visitorApiOsVersion: d.osVersion
    });
  }

  function pushError(code, message) {
    push({ event: "visitor-api-error", visitorApiErrorCode: code, visitorAPIErrorMessage: message });
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
    pending = pending || fetchData()
      .then(function (d) { pushSuccess(d); return d; })
      .catch(function (e) { pushError(500, e.message); throw e; });
    if (onSuccess || onError) {
      pending.then(function (d) { onSuccess && onSuccess(d); })
             .catch(function (e) { onError && onError(500, e.message); });
    }
    return pending;
  };
})(window);
