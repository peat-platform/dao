'use strict';
var minify = require('jsonminify');
var crypto = require('crypto');

function _sort(json, depth, til) {
   if(depth > til) {
      throw new Error('Maximal object depth reached');
   }
   if(typeof json !== 'object' || Array.isArray(json)) {
      return json;
   }
   var r = {};
   Object.getOwnPropertyNames(json).sort().forEach(function(e) {
      r[e] = _sort(json[e], depth + 1, til);
   });
   return r;
}

function sort(json) {
   return _sort(json, 0, 4);
}

function norm(json) {
   return JSON.minify(JSON.stringify(sort(json)));
}

function hash(str) {
   return crypto.createHash('md5').update(str).digest('hex') + '-' + str.length;
}

function isNotVowel(chr) {
   switch(chr)
   {
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
   size = (typeof size !== 'number' ? str.length : size);
   return String(str).toLowerCase().replace(/[^a-z0-9]/g,'').split('').filter(isNotVowel).splice(0, size).join('');
}

function abbreviate(u, size) {
   size = (typeof size !== 'number' ? 5 : size);
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


exports.hash = hash;
exports.sort = sort;
exports.norm = norm;
