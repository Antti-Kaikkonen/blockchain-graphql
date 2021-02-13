# Using BlockchainGraphQL API inside an Angular App

We create a simple Angular app and demonstrate how to generate strongly typed functions from BlockchainGraphQL queries.

1. `sudo npm install -g @angular/cli` ([more info](https://cli.angular.io/))
3. `ng new angular-blockchain-app`
    1. **Q:** Do you want to enforce stricter type checking and stricter bundle budgets in the workspace? **A:** `y`
    2. **Q:** Would you like to add Angular routing? **A:** `n`
    3. **Q:** Which stylesheet format would you like to use? **A:** Choose what you prefer. I prefer `SCSS`.
4. `cd angular-blockchain-app`
5. `ng add apollo-angular`
    1. **Q:** Url to your GraphQL endpoint **A:** `https://blockchaingraphql.com`
6. `npm install @graphql-codegen/cli --save-dev` ([more info](https://graphql-code-generator.com/docs/getting-started/index))
7. `npx graphql-codegen init`
    1. **Q:** What type of application are you building? **A:** `Application built with Angular`
    2. **Q:** Where is your schema? **A:** `https://blockchaingraphql.com`
    3. **Q:** Where are your operations and fragments? **A:** `src/**/*.graphql` (default)
    4. **Q:** Pick plugins **A:** `TypeScript`, `TypeScript Operations` and `TypeScript Apollo Angular`
    5. **Q:** Where to write the output **A:** `src/generated/graphql.ts` (default)
    6. **Q:** Do you want to generate an introspection file? **A:** `n`
    7. **Q:** How to name the config file? **A:** `codegen.yml` (default)
    8. **Q:** What script in package.json should run the codegen? **A:** `generate`
8. `npm install` to install the plugins
9. create `src/example.graphql` with the following content:
```GraphQL
query blockByHeight($coin: String!, $height: Int!) {
  coin(name: $coin) {
    blockByHeight(height: $height) {
      block {
        height
        hash
        txCount
      }
    }
  }
}
```
10. `npm run generate` to generate TypeScript code from the graphql schema and queries
11. edit `src/app/app.component.ts` to query information about a block:
```TypeScript
import { Component } from '@angular/core';
import { Observable } from 'rxjs';
import { BlockByHeightGQL, BlockByHeightQuery } from 'src/generated/graphql';
import { map } from 'rxjs/operators';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent {

  title="Block 100k";

  blockDetailsObs: Observable<BlockByHeightQuery>;

  constructor(blockByHeight: BlockByHeightGQL) {
    this.blockDetailsObs = blockByHeight.watch({height: 100000, coin:"bitcoin"})
        .valueChanges
        .pipe(
            map(result => result.data)
        );
  }

}

```
12. edit `src/app/app.component.html` to display the queried information
```html
<span>{{ title }} app is running!</span>
<div *ngIf="blockDetailsObs | async as blockDetails; else loading">
  <div *ngIf="blockDetails.coin as coin; else coin_not_found">
    <div *ngIf="coin.blockByHeight as blockByHeight; else block_not_found">
      <p>Height: {{blockByHeight.block.height}}</p>
      <p>Hash: {{blockByHeight.block.hash}}</p>
      <p>Tx count: {{blockByHeight.block.txCount}}</p>
    </div>
    <ng-template #block_not_found>
      <div>Block not found</div>
    </ng-template>
  </div>
  <ng-template #coin_not_found>
    <div>Coin not found</div>
  </ng-template>
</div>
<ng-template #loading>
  <div>Loading ...</div>
</ng-template>
```
13. `ng serve` to run the app. Open http://localhost:4200/ in your browser and you should see the following
```
Block 100k app is running!

Height: 100000

Hash: 000000000003ba27aa200b1cecaad478d2b00432346c3f1f3986da1afd33e506

Tx count: 4
```
