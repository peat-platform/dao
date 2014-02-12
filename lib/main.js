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

var dao = function(params) {

   helper.init(params.logger_params)

	var mongPush = zmq.bindToMong2PubQ({
		spec : params.mongrel_sub_q.spec,
		id   : params.mongrel_sub_q.id
	})

	zmq.bindToPullQ({
		spec : params.dao_sub_q.spec,
		id   : params.dao_sub_q.id
	}, function(msg) {

		helper.evaluateMessage(msg, function(err, results){

         msg.mongrel_resp.data.dao_out = results

         var response = zmq.Response(zmq.status.OK_200, zmq.header_json, msg.mongrel_resp)

         for (var i=0; i < msg.clients.length; i++){
            var client = msg.clients[i]
            mongPush.publish(client.uuid, client.connId, response)
         }
      });
	})
}

module.exports = dao