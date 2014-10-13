'use strict';

var Lazy = require('lazy.js');
var url = require('url');
var config = require('./config.js');

function invertType(typ) {
   typ = Lazy(typ);
   var tp = typ.get(config.type.property.prefix);
   var r = {};
   r[config.type.property.prefix] = Lazy(tp).map(function(v, k) {
      return [v[config.type.property.id.prefix] + config.object.property.seperator + v[config.type.property.type.prefix], k];
   }).toObject();
   return r;
}

function merger(a, b) {
   var r;
   if (Array.isArray(a) && Array.isArray(b)) {
      return a.concat(b);
   } else if (Array.isArray(a)) {
      r = [];
      r = a.concat([]);
      r.push(b);
      return r;
   } else if (Array.isArray(b)) {
      r = [];
      r = b.concat([]);
      r.push(a);
      return r;
   } else if (typeof a === 'object' && typeof b === 'object') {
      r = {};
      var ps = Lazy(Object.getOwnPropertyNames(a)).concat(Object.getOwnPropertyNames(b)).uniq().toArray();
      for(var p in ps) {
         if(ps.hasOwnProperty(p)) {
            r[ps[p]] = merger(a[ps[p]], b[ps[p]]);
         }
      }
      return r;
   } else if (typeof a === typeof b) {
      return b;
   } else if (typeof a === 'undefined') {
      return b;
   }
   //else if(typeof b === 'undefined')
   //	return a;
   return a;
}

function Evolution() {

}

Evolution.prototype = {};

Evolution.prototype.create = function(obj, typ) {
   var tp = typ[config.type.property.prefix];
   return Lazy(obj).pick(Object.getOwnPropertyNames(tp)).map(function(v, k) {
      return [tp[k][config.type.property.id.prefix] + config.object.property.seperator + tp[k][config.type.property.type.prefix], v];
   });
};

Evolution.prototype.read = function(obj, typ) {
   var tp = invertType(typ)[config.type.property.prefix];
   return Lazy(obj).pick(Object.getOwnPropertyNames(tp)).map(function(v, k) {
      return [tp[k], v];
   });
};

Evolution.prototype.update = function(obj1, obj2, typ) {
   console.log(obj1);
   obj2 = this.create(obj2, typ).toObject();
   console.log(obj2);
   var m = this.merge(obj1, obj2);
   console.log(m.toObject());
   return m;
};

Evolution.prototype.delete = function(obj, typ) {
   var tp = invertType(typ)[config.type.property.prefix];
   return Lazy(obj).omit(Object.getOwnPropertyNames(tp)).map(function(v, k) {
      return [tp[k], v];
   });
};

Evolution.prototype.merge = function(obj1, obj2) {
   return Lazy(obj1).merge(obj2, merger);
};

// //var foo = {'c': '0', 'a': '1', 'b': ['2', '3']};
// var bar = {'a': 'a', 'b': {'zoo': 'doh', 'goo': {'zap': 'snap'}, 'moo': {'maa': 'meh'}}};
// var baz = {'a': 'a', 'b': {'goo': undefined, 'moo': {'maa2': 'meh2'}}};

// //console.log(Lazy(bar).merge(foo, merger).toObject());
// //console.log(Lazy(foo).merge(bar, merger).toObject());

// console.log(Lazy(bar).merge(baz, merger).toObject());
// console.log(Lazy(baz).merge(bar, merger).toObject());

// //console.log(foo);
// console.log(bar);
// console.log(baz);

//var t1 = {'@context':{'name':{'@id':'dbp:Name', '@type':'openi:string'}}, '@type': 'dbp:Person'};
//var t2 = {'@context':{'name':{'@id':'dbp:Name', '@type':'openi:string'}, 'age':{'@id':'dbp:Age','@type':'openi:integer'}}, '@type': 'dbp:Person'};
//var t3 = {'@context':{'name':{'@id':'dbp:Full_Name', '@type':'openi:string'}, 'age':{'@id':'dbp:Age','@type':'openi:string'}}, '@type': 'dbp:Person'};

//var o1 = {'@type':'t1', 'name':'John Doe'};
//var o2 = {'@type':'t2', 'name':'John Doe', 'age':'42'};
//var o3 = {'@type':'t1', 'name':'Jane Doe'};

//var x1 = encodeObject(o1, t1);
//var x2 = encodeObject(o2, t2);
//var x3 = encodeObject(o3, t3);

//console.log(x1);
//console.log(x2);
//console.log(x3);

//console.log(decodeObject(x1, t1));
//console.log(decodeObject(x2, t2));
//console.log(decodeObject(x3, t3));

module.exports = new Evolution();