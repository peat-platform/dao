/*
 * DAO
 *
 *
 * Copyright (c) 2013 dconway, dmccarthy
 * Licensed under the MIT license.
 */

'use strict';

var zmq    = require('m2nodehandler')
var helper = require('./helper.js')


var mongrel_handlers = {}

var getMongrelSink = function(mongrel_sink){

   return (
      function(mongrel_sink){

         if (!mongrel_handlers[mongrel_sink.spec]){
            mongrel_handlers[mongrel_sink.spec] = zmq.sender(mongrel_sink)
         }

         return mongrel_handlers[mongrel_sink.spec]

      }(mongrel_sink, mongrel_handlers)
   )
}


var dao = function(config) {

   helper.init(config.logger_params)

	zmq.receiver(config.dao_sink, null, function(msg) {

      var mongSink = getMongrelSink(msg.mongrel_sink)

		helper.evaluateMessage(msg, function(err, results){

         if ( undefined === msg.mongrel_resp.data ){
            msg.mongrel_resp.data = {}
         }

         msg.mongrel_resp.data.dao_out = results

         var response = zmq.Response(zmq.status.OK_200, zmq.header_json, msg.mongrel_resp)

         for (var i=0; i < msg.clients.length; i++){
            var client = msg.clients[i]
            mongSink.send(client.uuid, client.connId, response)
         }
      });
	})
}

module.exports = dao