/*
 * DAO
 *
 *
 * Copyright (c) 2013 dconway, dmccarthy
 * Licensed under the MIT license.
 */

'use strict';

var zmq                 = require('m2nodehandler');
var helper              = require('./CBasehelper.js');
var subscriptionHandler = require('./subscriptions');
var HTTPStatus          = require('http-status');


var mongrel_handlers = {};

var getMongrelSink = function(mongrel_sink) {
   return (
      function(mongrel_sink) {

         if (!mongrel_handlers[mongrel_sink.spec]) {
            mongrel_handlers[mongrel_sink.spec] = zmq.sender(mongrel_sink);
         }

         return mongrel_handlers[mongrel_sink.spec];

      }(mongrel_sink, mongrel_handlers)
      );
};


var dao = function(config) {

   helper.init(config.logger_params);
   subscriptionHandler.init((config));

   zmq.receiver(config.dao_sink, null, function(msg) {

      var mongSink = getMongrelSink(msg.mongrel_sink);

      helper.evaluateMessage(msg, function (err, results) {

         var httpStatusCode = HTTPStatus.OK;
         var resp           = {};
         var headers        = zmq.standard_headers.json;

         if (1 === Object.keys(results).length) {
            resp             = results[0][0];
            httpStatusCode   = results[0][1];
            if(undefined !== results[0][2]){
               headers = results[0][2];
            }
         } else {
            httpStatusCode   = results[0][1];
            headers          = results[0][2];
            resp             = [];
            for ( var i in results ) {
               resp[i] = results[i][0];
            }
         }

         for (var i=0; i < msg.clients.length; i++) {
            var client = msg.clients[i];
            if (resp !== undefined) {
               subscriptionHandler.checkSubscription(msg, httpStatusCode, resp);
            }
            //subpush.send({'msg' : msg, 'status': httpStatusCode, 'response' : resp})
            mongSink.send(client.uuid, client.connId, httpStatusCode, headers, resp);
         }
      });
   });
};

module.exports = dao;