/**
 * Created by dmccarthy on 22/08/2014.
 */

var logger = null;

var design_docs = {
   objects_views: {
      views: {
         object_by_cloudlet_id: {
            map: function(doc, meta) {
               var parts = meta.id.split('+');
               if(parts.length > 1){
                  emit(parts[0]+"_id", doc._id);
                  emit(parts[0], doc);
               }
            }
         },
         object_by_type: {
            map: function(doc, meta) {
               var parts = meta.id.split('+');
               if( parts.length > 1 ){
                  emit( parts[0] + "_id+" + doc._openi_type, doc._id );
                  emit( parts[0] + "+"    + doc._openi_type, doc );
               }
            }
         }
      }
   }
}


function createViewsForDesignDoc(db, localdocName, localdocValue){

   db.getDesignDoc( localdocName, function( err, ddoc, meta ) {

      if ( null !== err && 4104 === err.code){
         db.setDesignDoc( localdocName, localdocValue, function( err, res ) {
            logger.log("INFO", "Created Whole Design Doc: " + localdocName)
         })
      }
      else{
         for ( var i in localdocValue.views){

            if (!(i in ddoc['views'])){
               ddoc.views[i] = localdocValue.views[i];

               db.setDesignDoc( localdocName, ddoc, function( err, res ) {
                  logger.log("INFO", 'Create view ' + i + ' for Design Doc ' + localdocName )
               })
            }
         }
      }
   });
}


var createDesignDocs = function(db){
   for ( var i in design_docs){
      createViewsForDesignDoc(db, i, design_docs[i])
   }
}


module.exports = function(db, loggerObj){

   logger = loggerObj;

   //stringify all views map functions
   for ( var i in design_docs ){
      for ( var j in design_docs[i]['views']){
         design_docs[i]['views'][j].map = '' + design_docs[i]['views'][j].map
      }
   }

   createDesignDocs(db);
}
