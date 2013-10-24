# dao

The Cloudlet Platforms dao component persists data and retrieves it from the datastore. At the moment it is just a
demonstrator so it destroys and recreates the database each time it is restarted.

## Getting Started
Install the module with: `npm install git+ssh://git@gitlab.openi-ict.eu:dao.git`

You will need to install the following through macports or aptitude.

```bash
sudo port install JsCoverage
sudo port install phantomjs
```

or

```bash
sudo apt-get install JsCoverage
sudo apt-get install phantomjs
```

To build the project enter the following commands. Note: npm install is only required the first time the module is
built or if a new dependency is added. There are a number of grunt tasks that can be executed including: test, cover,
default and jenkins. The jenkins task is executed on the build server, if it doesn't pass then the build will fail.

```bash
git clone git@gitlab.openi-ict.eu:dao.git
cd dao
npm install
grunt jenkins
```

To start the component enter:

```javascript
node lib/main.js
```

## Documentation

Sample incoming message to PUT data into datastore

```javascript
{
 uuid: '81b7114c-534c-4107-9f17-b317cfd59f62',
  connId: '24',
  action: 'PUT',
  name: 'asdasd',
  data: { vip_data: 42 }
}

```

Output

```javascript
{
 "result":"success"
}
```

Sample incoming message GET data.

```javascript
{
  uuid: '81b7114c-534c-4107-9f17-b317cfd59f62',
  connId: '23',
  action: 'GET',
  name: 'asdasd',
  data: null
}
```

Output

```javascript
{
    result: "success",
    value: {
        _id: "asdasd",
        _rev: "1-28c802f1f5e6569dffff096784d98b8c",
        vip_data: 42
    }
}
```

## Contributors

* Donal McCarthy (dmccarthy@tssg.org)


## Release History
**0.1.0** *(23/10/14 dmccarthy@tssg.org)* First version of the dao module pushes the data into a couchDB document store and retrieves it.


## License
Copyright (c) 2013
Licensed under the MIT license.
