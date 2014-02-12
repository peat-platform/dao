'use strict';

var base_path = require('./basePath.js');
var dao       = require(base_path + '../lib/helper.js');
var nano      = require('nano')('http://localhost:5984');
var net       = require('net')
var zmq       = require('m2nodehandler')


/*
  ======== A Handy Little Nodeunit Reference ========
  https://github.com/caolan/nodeunit

  Test methods:
    test.expect(numAssertions)
    test.done()
  Test assertions:
    test.ok(value, [message])
    test.equal(actual, expected, [message])
    test.notEqual(actual, expected, [message])
    test.deepEqual(actual, expected, [message])
    test.notDeepEqual(actual, expected, [message])
    test.strictEqual(actual, expected, [message])
    test.notStrictEqual(actual, expected, [message])
    test.throws(block, [error], [message])
    test.doesNotThrow(block, [error], [message])
    test.ifError(value)
*/


exports['init'] = {
   setUp: function(done) {
      done();
   },
   'no args': function(test) {

      var params = {
         'path'      : '/opt/openi/cloudlet_platform/logs/dao',
         'log_level' : 'debug',
         'as_json'   : true
      }

      dao.init(params);

      test.done();
   }
};


exports['create'] = {
   setUp: function (callback) {
      var params = {
         'action'       : 'CREATE',
         'cloudlet'     : 'c_3423423423423431',
         'object_name'  : "test",
         'object_data'  : {'a':true},
         'mongrel_resp' : {'value':true, 'other' : 'abc'}
      }

      dao.evaluateMethod(params, function(){});
      callback();
   },
   tearDown: function (callback) {
      var params = {
         'action'       : 'DELETE',
         'cloudlet'     : 'c_3423423423423431',
         'object_name'  : "test",
         'object_data'  : {'a':true},
         'mongrel_resp' : {'value':true, 'other' : 'abc'}
      }

      dao.evaluateMethod(params, function(){});
      callback();
   },
   'test malformed object': function(test) {

      test.throws(function(){
         var params = {
            'path'      : '/opt/openi/cloudlet_platform/logs/dao',
            'log_level' : 'debug',
            'as_json'   : true
         }

         dao.evaluateMethod(params, function(out){
         });

      })
      test.done();
   },
   'test create': function(test) {

      var params = {
         'action'       : 'CREATE',
         'cloudlet'     : 'c_3423423423423434',
         'object_name'  : "test",
         'object_data'  : {'a':true},
         'mongrel_resp' : {'value':true, 'other' : 'abc'}
      }

      dao.evaluateMethod(params, function(out){
         test.equal(200, out.status)
         test.equal({ 'Content-Type': 'application/json; charset=utf-8' }, out.headers)
         test.equal({'value':true, 'other' : 'abc'}, out.body)


         params.action = 'DELETE'
         dao.evaluateMethod(params);
      });

      test.done();
   },
   'test put': function(test) {

      var params = {
         'action'       : 'PUT',
         'cloudlet'     : 'c_3423423423423431',
         'object_name'  : "test",
         'object_data'  : {'a':true},
         'mongrel_resp' : {'value':true, 'other' : 'abc'}
      }

      dao.evaluateMethod(params, function(out){
         console.log("!!!!!!!!!!!!!!!")
         console.log(out)
         console.log(out.status)
         test.equal(200, out.status)
         test.equal({ 'Content-Type': 'application/json; charset=utf-8' }, out.headers)
         test.equal({"value":true,"other":"abc"}, out.body)
      });

      test.done();
   },
   'test get': function(test) {

   var params = {
      'action'       : 'DELETE',
      'cloudlet'     : 'c_3423423423423431',
      'object_name'  : "test",
      'object_data'  : {'a':true},
      'mongrel_resp' : {'value':true, 'other' : 'abc'}
   }

   dao.evaluateMethod(params, function(out){
      console.log("!!!!!!!!!!!!!!!")
      console.log(out)
      console.log(out.status)
      test.equal(200, out.status)
      test.equal({ 'Content-Type': 'application/json; charset=utf-8' }, out.headers)
      test.equal({"value":true,"other":"abc"}, out.body)
   });
   test.done();
},
   'test delete': function(test) {

      var params = {
         'action'       : 'CREATE',
         'cloudlet'     : 'c_3423423423423434',
         'object_name'  : "test",
         'object_data'  : {'a':true},
         'mongrel_resp' : {'value':true, 'other' : 'abc'}
      }

      dao.evaluateMethod(params, function(out){

         var params = {
            'action'       : 'DELETE',
            'cloudlet'     : 'c_3423423423423434',
            'object_name'  : "test",
            'object_data'  : {'a':true},
            'mongrel_resp' : {'value':true, 'other' : 'abc'}
         }

         dao.evaluateMethod(params, function(out){
            test.equal(200, out.status)
            test.equal({ 'Content-Type': 'application/json; charset=utf-8' }, out.headers)
            test.equal({"value":true,"other":"abc"}, out.body)
         });
      });
      test.done();
   }
};
