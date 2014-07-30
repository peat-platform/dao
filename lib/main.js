/*
 * DAO
 *
 *
 * Copyright (c) 2013 dconway, dmccarthy
 * Licensed under the MIT license.
 */

'use strict';

var zmq        = require('m2nodehandler')
var helper     = require('./CBasehelper.js')
var HTTPStatus = require('http-status');



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

   var subpush = zmq.sender(config.sub_sink)

	zmq.receiver(config.dao_sink, null, function(msg) {

      var mongSink = getMongrelSink(msg.mongrel_sink)

      helper.evaluateMessage(msg, function (err, results) {

         var httpStatusCode = HTTPStatus.OK;

         if ( 1 === Object.keys(results).length ){
            for(var resultsEntry in results) {
               msg.mongrel_resp = results[resultsEntry][0];
               httpStatusCode   = results[resultsEntry][1];
               break;
            }
         }
         else{
            httpStatusCode   = results[0][1];
            msg.mongrel_resp = results;
         }

         var response = zmq.Response(httpStatusCode, zmq.header_json, msg.mongrel_resp)

         for (var i=0; i < msg.clients.length; i++){
            var client = msg.clients[i]
            mongSink.send(client.uuid, client.connId, response)
         }

      });

	})
}

module.exports = dao
