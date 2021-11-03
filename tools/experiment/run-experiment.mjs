#!/usr/bin/env zx

cd('./../')

const wsPath = '/Users/ty/working/flink-example-fraud-detection/word-stream'
const batched_jar = 'minibatch-wordcount.jar'
const original_jar = 'original-wordcount.jar'
const flink = './'

const jars = [batched_jar, original_jar]

const rate = 10000

async function run_experiment(rate) {

    for (const jar of jars) {
        let ws = nothrow($`go run ${wsPath}/main.go ${rate} ${wsPath}/Chicken_Soup_for_the_Soul.txt`)

        for await (let chunk of ws.stderr) {
            if (chunk.includes("listening for connections")) break
        }

        let run_flink = $`./build-target/bin/flink run ${jar}`
        await sleep(1000)
        ws.kill('SIGINT')

    }


}

