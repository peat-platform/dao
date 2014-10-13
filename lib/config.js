var config =
   Object.freeze({
      object:
      {
         prefix: "object-",
         property: {
            prefix: "properties",
            seperator: ",",
            type: {
               prefix: "@type"
            },
         }
      },
      type:
      {
         prefix: "type-",
         property:
         {
            prefix: "@context",
            type: {
               prefix: "@type"
            },
            id:
            {
               prefix: "@id"
            }
         }
      },
      omap:
      {
         prefix: "omap-"
      },
      tmap:
      {
         prefix: "tmap-"
      },
      user:
      {
         prefix: "user-"
      },
      application:
      {
         prefix: "application-"
      },
      token:
      {
         prefix: "token-"
      }
   });

module.exports = config;