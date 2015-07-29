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
         'path'      : '/opt/peat/cloudlet_platform/logs/dao',
         'log_level' : 'debug',
         'as_json'   : true
      }

      dao.init(params);

      test.done();
   }
};


exports['create'] = {

   'test get': function(test) {

      var msg = {
         'dao_actions'      : [
            { 'action' : 'CREATE', 'database': 'aaa' },
            { 'action' : 'POST',   'database': 'aaa', 'object_name'  : "b", 'object_data'  : {"v":123}},
            { 'action' : 'GET',    'database': 'aaa', 'object_name'  : "b" },
            { 'action' : 'PUT',    'database': 'aaa', 'object_name'  : "b", 'object_data'  : {"b":true},
               'revision' : '1-24f52e652db87f025a69db3efb764e02'},
            { 'action' : 'FETCH',  'database': 'aaa', 'object_name'  : {} },
            { 'action' : 'DELETE', 'database': 'aaa' },
            { 'action' : 'PUT',    'database': 'bbb', 'object_name'  : "b", 'object_data'  : {"b":true},
               'revision' : '1-24f52e652db87f025a69db3efb764e02'},
            { 'action' : 'GET',    'database': 'bbb', 'object_name'  : "b" },
            { 'action' : 'FETCH',  'database': 'bbb', 'object_name'  : {} },
            { 'action' : 'CREATE', 'database': 'xxx' },
            { 'action' : 'CREATE', 'database': 'xxx' },
            { 'action' : 'DELETE', 'database': 'xxx' },
            { 'action' : 'DELETE', 'database': 'xxx' }
         ],
         'mongrel_resp' : {'value':true, 'data': {}},
         'clients'      : [
            {'uuid' : "ABC", 'connId' : "23423" }
         ]
      }

      var out = dao.evaluateMessage(msg, function( a, resp ){
//
//         console.log('++++++++++++++++');
//         console.log(resp);
//         console.log('++++++++++++++++');

         test.deepEqual({ value: true, data: 'Database Created: aaa' }, resp['0'])
         test.done();
      })


}
};
