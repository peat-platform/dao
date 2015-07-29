/*
 * DAO
 *
 *
 * Copyright (c) 2013 dconway, dmccarthy
 * Licensed under the MIT license.
 */

'use strict';

var zmq                 = require('zmq');
var zmqM2Node           = require('m2nodehandler');
var helper              = require('./CBasehelper.js');
var perms_prop_worker   = require('./perms_propogator_worker');
var subscriptionHandler = require('./subscriptions');
var HTTPStatus          = require('http-status');
var loglet              = require('loglet');
loglet                  = loglet.child({component: 'dao'});


var mongrel_handlers = {};

var getMongrelSink = function(mongrel_sink) {
   return (
      function(mongrel_sink) {
         if (undefined === mongrel_sink || "" === mongrel_sink){
            return {send:function(){}};
         }

         if (!mongrel_handlers[mongrel_sink.spec]) {
            mongrel_handlers[mongrel_sink.spec] = zmqM2Node.sender(mongrel_sink);
         }

         return mongrel_handlers[mongrel_sink.spec];

      }(mongrel_sink, mongrel_handlers)
      );
};


var dao = function(config) {

   helper.init(config.logger_params, config.tracklet_worker, config.perms_propagator_f);
   subscriptionHandler.init((config));

   zmqM2Node.receiver(config.dao_sink, null, function(msg) {

      var mongSink = getMongrelSink(msg.mongrel_sink);

      helper.evaluateMessage(msg, function (err, results) {
         if(err) {
            loglet.error(err);
         }
         var httpStatusCode = HTTPStatus.OK;
         var resp           = {};
         var headers        = zmqM2Node.standard_headers.json;

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
            mongSink.send(client.uuid, client.connId, httpStatusCode, headers, resp);
         }
      });
   });
};

dao['dao_proxy'] = function(config){

   //frontend should/could be router & backend should be dealer
   var frontend = zmq.socket('pull');
   var backend  = zmq.socket('push');

   frontend.bindSync(config.frontend);
   backend.bindSync(config.backend);

   frontend.on('message', function(msg) {
      backend.send(msg);
   });
};


dao['perms_prop_worker'] = perms_prop_worker.init;

module.exports = dao;