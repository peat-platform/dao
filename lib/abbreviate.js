'use strict';

var Lazy = require('lazy.js');
var url = require('url');

function isNotVowel(chr) {
   switch(chr) {
      case 'a':
      case 'e':
      case 'i':
      case 'o':
      case 'u':
         //case 'y':
         //case 'h':
         //case 'w':
         return false;
   }
   return true;
}

function consonants(str, size) {
   var size = (typeof size !== 'number' ? str.length : size);
   return String(str).toLowerCase().replace(/[^a-z0-9]/g,'').split('').filter(isNotVowel).splice(0, size).join('');
}

function abbreviate(u, size) {
   var host;
   var size = (typeof size !== 'number' ? 5 : size);
   u = (typeof ustr !== 'object') ? url.parse(u) : u;
   host = u.hostname.split('.');
   if(host.length < 2) {
      throw new Error('Expecting authority with toplevel domain URL (see rfc3986)!');
   }
   host = host[host.length - 2];
   if(u.protocol === null) {
      throw new Error('URI must have a protocol (see rfc3986)!');
   }

   //if(host.length < size)
   //	return host;

   return consonants(host);
   /*
    var hc = consonants(host);
    if (hc.length <= size)
    return hc;
    else
    return hc.slice(0, size) + hc.length;
    */
}

/*
 console.log(abbreviate('http://en.wikipedia.org/wiki/Soundex'));
 console.log(abbreviate('http://en.wikipedia.org'));
 console.log(abbreviate('http://de.wikipedia.org', 17));

 console.log(abbreviate('http://wikipedia.org'));
 console.log(abbreviate('http://dbpedia.org'));
 console.log(abbreviate('http://schema.org'));
 console.log(abbreviate('http://xmlns.org'));
 console.log(abbreviate('http://purl.org'));
 console.log(abbreviate('http://preoductontology.org', 4));
 console.log(abbreviate('http://w3.org'));
 console.log(abbreviate('http://data-vocabulary.org', 4));
 console.log(abbreviate('http://productontology.org'));
 console.log(abbreviate('http://w3.org'));
 console.log(abbreviate('http://data-vocabulary.org'));
 */

function commonPrefixes(strs) {
   return strs.sortBy(Lazy.identity).toArray();
}

console.log(commonPrefixes(Lazy(["http://purl.org/dc/elements/1.1/", "http://example.org/vocab#", "http://xmlns.com/foaf/0.1/homepage", "http://xmlns.com/foaf/0.1/name", "http://www.w3.org/2001/XMLSchema#", "http://www.w3.org/2002/12/cal/ical#", "http://purl.org/goodrelations/v1#", "http://www.productontology.org/id/", "http://rdf.data-vocabulary.org/#name", "http://rdf.data-vocabulary.org/#ingredients", "xsd:integer", "http://www.w3.org/2001/XMLSchema#"])));

exports.abbreviate = abbreviate;