import { Injectable } from '@angular/core';
import { Http, Response } from '@angular/http';
import { Observable } from 'rxjs/Observable';
import 'rxjs/add/operator/map';
import 'rxjs/Rx';
import { IProduct } from './product';
import { IUser, IServer, ISystem} from './user';
import { IUserDetail } from './userdetail';

@Injectable()
export class ProductService {
    private _productUrl = 'api/products/products.json';
    private _productUrlUser = 'http://jsonplaceholder.typicode.com/users/1';
	private _productUrlUser1 = 'http://jsonplaceholder.typicode.com/posts';
	private _usersUrl = 'http://jsonplaceholder.typicode.com/users';
    private _serversUrl = 'cm/server';
    private _systemsUrl = 'cm/system';
	
    
    constructor(private _http: Http) { }
    
    getServers() : Observable<IServer[]> {
        return this._http.get(this._serversUrl)
        .map(res=>res.json())
    }


    getsystemsbyserverId(serverId : Number) : Observable<ISystem> {
       return this._http.get(this._systemsUrl + '/' + serverId); 
    }

    getUserDetail() : Observable<IUserDetail[]>{	  
      return this._http.get(this._usersUrl)
	  .map(( response: Response) => <IUserDetail[]> response.json())
    };	

    getUser1() : Observable<IUser[]> {
      return this._http.get(this._productUrlUser1)
            .map((response: Response) => <IUser[]> response.json())
            .do(data => console.log('UserData: ' +  JSON.stringify(data)))			
    };	

	
    getProducts(): Observable<IProduct[]> {
        return this._http.get(this._productUrl)
            .map((response: Response) => <IProduct[]> response.json())
            .do(data => console.log('All: ' +  JSON.stringify(data)))
            .catch(this.handleError);
    }

    getProduct(id: number): Observable<IProduct> {
        return this.getProducts()
            .map((products: IProduct[]) => products.find(p => p.productId === id));
    }

    getUser(id: number): Observable<IUser> {
        return this.getUser1()
            .map((users: IUser[]) => users.find(p => p.id === id));
    }
	
	
    private handleError(error: Response) {
        // in a real world app, we may send the server to some remote logging infrastructure
        // instead of just logging it to the console
        console.error(error);
        return Observable.throw(error.json().error || 'Server error');
    }
}
