var async   = require("async");
var loglet  = require('loglet');


loglet = loglet.child({component: 'dao'});


var comparePerms = function(a, b){

   if ( undefined === b && undefined !== a ){
      return false
   }

   for (i in a){
      if (a[i] !== b[i]){
         return false
      }
   }

   for (i in b){
      if (a[i] !== b[i]){
         return false
      }
   }

   return true
}


var propagatePermissions = function(couchbase, dbs, msg, perms){

   var prevComplete     = (undefined !== perms._prev && perms._prev.status === 'propagated')
   var tag              = perms._current._date_created
   var object_updates   = {}
   var objects          = []
   var type_updates_app = {}
   var type_updates_all = {}
   var cloudlet_id      = msg.cloudlet_id;
   var thirdPartyId     = msg.third_party;


   var permsObjects     = perms._current.perms['@objects']
   var permsTypes       = perms._current.perms['@types']

   var prevPermsObjects = (undefined === perms._prev) ? {} : perms._prev.perms['@objects']
   var prevPermsTypes   = (undefined === perms._prev) ? {} : perms._prev.perms['@types']


   for (var i in permsTypes){
      if ( prevComplete ){
         if (permsTypes[i]['@app_level']){
            if ( undefined === prevPermsTypes[i] ||  !comparePerms(permsTypes[i]['@app_level'], prevPermsTypes[i]['@app_level']) ){
               type_updates_app[i] = permsTypes[i]['@app_level']
            }
         }
         if (permsTypes[i]['@cloudlet_level']){
            if ( undefined === prevPermsTypes[i] || !comparePerms(permsTypes[i]['@cloudlet_level'], prevPermsTypes[i]['@cloudlet_level']) ){
               type_updates_all[i] = permsTypes[i]['@cloudlet_level']
            }
         }
      }
      else{
         type_updates_app[i] = permsTypes[i]['@app_level']
         type_updates_all[i] = permsTypes[i]['@cloudlet_level']
      }
   }


   for (var i in permsObjects){
      if ( !prevComplete || !comparePerms(permsObjects[i], prevPermsObjects[i]) ){
         object_updates[i] = permsObjects[i]
         objects.push(i)
      }
   }


   for ( var type_id in type_updates_all ){
      var permissions = type_updates_all[type_id]
      updateObjectPermissionsByType(couchbase, dbs, cloudlet_id, type_id, thirdPartyId, permissions, objects, false)
   }

   for ( var type_id in type_updates_app ){
      var permissions = type_updates_app[type_id]
      updateObjectPermissionsByType(couchbase, dbs, cloudlet_id, type_id, thirdPartyId, permissions, objects, true)
   }

   //not async between 2
   //process objects last

   var funs = {}

   for ( var object_id in object_updates ){
      var permissions = object_updates[object_id]
      funs[i] = function(dbs, cloudlet_id, object_id, thirdPartyId, permissions, applevel){
         return function (callback) {
            updateObjectsPermission(dbs, cloudlet_id, object_id, thirdPartyId, permissions, applevel, callback)
         }
      }(dbs, cloudlet_id, object_id, thirdPartyId, permissions, false)
   }


   async.series(funs, function (err, results) {

      if (err) {
         loglet.error(err);
      }
      else{

         dbs['permissions'].get(msg.database, function (err, db_body) {

            if (err && 13 === err.code){
               loglet.info("Permissions do not exist: update failed");
            }
            else{
               if ( tag === db_body.value._current._date_created){
                  db_body.value._current.status = "propagated"
               }
               else if ( tag === db_body.value._prev._date_created){
                  db_body.value._prev.status = "propagated"
               }

               dbs['permissions'].replace(msg.database, db_body.value, function (err, result) {
                  loglet.info("Permissions have been propogated");
               })
            }
         });
      }
   });
}


var updateObjectPermissionsByType = function(couchbase, dbs, cloudlet_id, type_id, thirdPartyId, permissions, objects, applevel){

   var skey = [cloudlet_id, type_id]
   var ekey = [cloudlet_id, type_id + "^"]


   applevel = true

   var ViewQuery = couchbase.ViewQuery;
   var query = ViewQuery.from('objects_views', 'object_by_type')
      .stale(ViewQuery.Update.BEFORE)
      .range(skey, ekey, true)
      .reduce(false)


   dbs['objects'].query(query, function (err, res) {

      if (err){
         return
      }

      var funs = {}

      for (var i = 0; i < res.length; i ++){
         var object_id = res[i].value[1]

         if ( -1 === objects.indexOf(object_id) ){
            funs[i] = (function(dbs, cloudlet_id, object_id, thirdPartyId, permissions, applevel){
               return function (callback) {
                  updateObjectsPermission(dbs, cloudlet_id, object_id, thirdPartyId, permissions, applevel, callback)
               }
            }(dbs, cloudlet_id, object_id, thirdPartyId, permissions, applevel))
         }
      }

      async.series(funs, function (err, results) {

         if (err) {
            loglet.error(err);
         }
         else{
            //console.log(results)
         }
      });

   })
}

var updateObjectsPermission = function(dbs, cloudlet_id, object_id, thirdPartyId, permissions, applevel, callback){

   var key = cloudlet_id + '+' + object_id

   dbs['objects'].get(key, function (err, db_body) {

      if(err) {
         loglet.error(err);
         callback(null, "not found: " + key)
         return
      }

      if ( undefined === db_body.value._permissions ){
         db_body.value._permissions = {}
      }

      if ( !applevel || (applevel && db_body.value._permissions.created_by === thirdPartyId) ){

         db_body.value._permissions[thirdPartyId] = permissions

         dbs['objects'].replace(key, db_body.value, function (err, result) {

            if (err) {
               loglet.error(err);

               callback(null, "failure: " + key)
            }
            else {
               callback(null, "success: " + key)
            }
         });
      }
      else{
         callback(null, "success: " + key)
      }
   });
}


var createPemissions = function(couchbase, dbs, msg, callback){

   var data = {
      _current : msg.object_data,
      _history : []
   }

   dbs[msg['bucket']].insert(msg.database, data, {}, function (err, result) {
      if (err) {
         loglet.error(err);
      }
      else {
         callback(null, { 'status': 'update' }, 200);
         if(msg['bucket']!== "app_permissions") {
            propagatePermissions(couchbase, dbs, msg, data)
         }
      }
   });
}


var updateExistingPemissions = function(couchbase, dbs, msg, db_body, callback){


   if ( undefined !== db_body.value['_prev'] && null !== db_body.value['_prev']){
      db_body.value['_history'].push(db_body.value['_prev'])
   }

   db_body.value['_prev']    = db_body.value['_current']
   db_body.value['_current'] = msg.object_data


   dbs[msg['bucket']].replace(msg.database, db_body.value, {}, function (err, result) {
      if (err) {
         loglet.error(err);
      }
      else {
         callback(null, { 'status': 'update' }, 200);
         propagatePermissions(couchbase, dbs, msg, db_body.value)

      }
   });
}

var createAppPemissions = function(couchbase, dbs, msg, callback){

   dbs[msg['bucket']].insert(msg.database, msg.object_data, {}, function (err, result) {
      if (err) {
         loglet.error(err);
      }
      else {
         callback(null, { 'status': 'update' }, 200);
      }
   });
}


var updateAppPemissions = function(couchbase, dbs, msg, db_body, callback){


   if ( undefined !== db_body.value['_prev'] && null !== db_body.value['_prev']){
      db_body.value['_history'].push(db_body.value['_prev'])
   }

   db_body.value['_prev']    = db_body.value['_current']
   db_body.value['_current'] = msg.object_data

   dbs[msg['bucket']].replace(msg.database, db_body.value, {}, function (err, result) {
      if (err) {
         loglet.error(err);
      }
      else {
         callback(null, { 'status': 'update' }, 200);
         if(msg['bucket']!== "app_permissions") {
            propagatePermissions(couchbase, dbs, msg, db_body.value)
         }
      }
   });
}


var processPermissionsUpdate = function(couchbase, dbs, msg, callback){

   // get permissions
   dbs[msg['bucket']].get(msg.database, function (err, db_body) {

      if (err && 13 === err.code){
         loglet.info("Permissions do not exist");
         createPemissions(couchbase, dbs, msg, callback)
      }
      else{
         updateExistingPemissions(couchbase, dbs, msg, db_body, callback)
      }
   });

}

var processAppPermissionsUpdate = function(couchbase, dbs, msg, callback){

   createAppPemissions(couchbase, dbs, msg, callback)

}


module.exports.processPermissionsUpdate    = processPermissionsUpdate;
module.exports.processAppPermissionsUpdate = processAppPermissionsUpdate;