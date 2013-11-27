'use strict';

var base_path = require('./basePath.js');
var DAO       = require(base_path + '../lib/helper.js');
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
var server = net.createServer()

server.on('connection', function (conn) {
  conn.on('data', function (data) {
        console.log(data);
    });
})
server.listen(5555)


exports['awesome'] = {
  setUp: function(done) {
    // setup here
    done();
  },
  'no args': function(test) {
    test.equal('awesome', 'awesome', 'should be awesome.');
    // tests here
    test.done();
  },
};

/*exports['getAction'] = {
  setUp: function(done) {
    this.mongPush = zmq.bindToMong2PubQ({spec:'tcp://127.0.0.1:5555', id:'dao_conn'})
    this.msg = {
      uuid   : "0123456789",
      connId : 0,
      action : "GET",
      name   : ["data","000001","test1"],
      data   : "null"
   }
    done();
  },
  'no args': function(test) {
    
    DAO.initDB()
    DAO.getAction(this.msg, this.mongPush)
    console.log(this.mongPush['publish'])
    test.done();
  }
};*/
