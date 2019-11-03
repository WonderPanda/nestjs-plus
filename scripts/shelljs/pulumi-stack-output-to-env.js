const shell = require('shelljs');
const program = require('commander');
const fs = require('fs');
const _ = require('lodash');
const path = require('path');

program
  .version('0.0.1')
  .option('-p, --pulumi-path <pulumiPath>', 'path to pulumi directory')
  .option('-s, --stack <stack>', 'the stack name to reference')
  .option('-o --out-dir [outDir]', 'the output path for the .env file')
  .parse(process.argv);

const { pulumiPath, stack, outDir } = program;

shell.cd(pulumiPath);
shell.exec(`pulumi stack select ${stack}`);
const stackOutput = shell.exec('pulumi stack output', {
  silent: true
}).stdout;

// Remove the first two lines from output based on the format:
// Current stack outputs (2):
//     OUTPUT                     VALUE

const lines = stackOutput.split('\n');
lines.splice(0, 2);

const envs = lines
  .filter(x => x)
  .map(x => {
    const [name, value] = x.split(' ').filter(x => x);
    return `${name}=${value}`;
  });

const stackEnv = envs.join('\n');

console.log(stackEnv);

const outFile = `${path.join(__dirname, outDir)}/.env`;
console.log(outFile);
fs.writeFile(outFile, stackEnv, () => {
  console.log(`Wrote pulumi stack ${stack} env to ${outFile}`);
});
