const cors = require('cors')
const express = require('express')
const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: [
    'my-kafka-0.my-kafka-headless.btoarriola.svc.cluster.local:9092'
	  ]
});

const producer = kafka.producer()

const app = express();
app.use(cors());
app.options('*', cors());

const port = 8080;

app.get('/', (req, res, next) => {
  res.send('kafka api - btoarriola');
});

//ESTE ES EL PRIMERO
const run = async (username) => {

  await producer.connect()
//    await producer.send()
  await producer.send({
    topic: 'test',
    messages: [ 
    { 
      'value': `{"name": "${username}" }` 
      } 
        ],
      })
    await producer.disconnect()
    }

    app.get('/like', (req, res, next) => {
    const username = req.query.name;
    res.send({ 'name' : username } );
    run(username).catch(e => console.error(`[example/producer] ${e.message}`, e))

    });

//ESTE ES EL DE REACCIONES
const runReactions = async (userid, objectid, reactionid) => {

    await producer.connect()
//    await producer.send()
    await producer.send({
      topic: 'reaction',
      messages: [
        {
          'value': `{"userid": "${userid}","objectid": "${objectid}","reactionid": "${reactionid}" }` 
        }
      ],
    })
    await producer.disconnect()
}

app.get('/reactions', (req, res, next) => {
  const userid = req.query.userid;
  const objectid = req.query.objectid;
  const reactionid = req.query.reactionid;
  res.send({ 'userid':userid, 'objectid': objectid, 'reactionid': reactionid });
  runReactions(userid, objectid, reactionid).catch(e => console.error(`[example/producer] ${e.message}`, e))

});


//ESTE ES EL DE COMENTARIOS
const runComments = async (userid, objectid, message) => {

  await producer.connect()
//    await producer.send()
  await producer.send({
    topic: 'comment',
    messages: [
      {
        'value': `{"userid": "${userid}","objectid": "${objectid}","message": "${message}" }` 
      }
    ],
  })
  await producer.disconnect()
}

app.get('/comments', (req, res, next) => {
const userid = req.query.userid;
const objectid = req.query.objectid;
const message = req.query.message;
res.send({ 'userid':userid, 'objectid': objectid, 'message': message });
runComments(userid, objectid, message).catch(e => console.error(`[example/producer] ${e.message}`, e))

});

app.listen(port,  () => 
	console.log('listening on port ' + port
));
