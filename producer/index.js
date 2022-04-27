import Kafka from 'node-rdkafka'
import eventType from '../eventType.js'

console.log("Producer..")

const stream = Kafka.Producer.createWriteStream(
  {
    'metadata.broker.list': 'localhost:9092'
  },
  {},
  {
    topic: 'test',
  }
)

const getRandomAnimal = () => {
  const categories = ['CAT', 'DOG'];
  return categories[Math.floor(Math.random() * categories.length)]
}

const getRandomNoise = (category) => {
  if (category === 'CAT') {
    const noises = [ 'Grrr', 'meow'];
    return noises[Math.floor(Math.random() * noises.length)]
  }else {
    const noises = [ 'woof', 'bark!'];
    return noises[Math.floor(Math.random() * noises.length)]
  }
}

const queueMessage = () => {

  const category = getRandomAnimal();
  const noise = getRandomNoise(category);
  const event = { category, noise }

  const successful = stream.write(eventType.toBuffer(event));
  if(successful){
    console.log('message wrote successfully to stream')
  }else {
    console.log('something went wrong')
  }
}

setInterval(()=>{
  queueMessage();
}, 3000)