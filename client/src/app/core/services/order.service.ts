import { inject, Service } from '@angular/core';
import { environment } from '../../../environments/environment.development';
import { HttpClient } from '@angular/common/http';
import { Order, OrderToCreate } from '../../shared/models/order';

@Service()
export class OrderService {
    baseUrl = environment.apiUrl;
    private http = inject(HttpClient);
    orderComplete = false;
    
    createOrder(OrderToCreate: OrderToCreate){
        return this.http.post<Order>(this.baseUrl + 'orders', OrderToCreate);
    }

    getOrdersForUser(){
        return this.http.get<Order[]>(this.baseUrl + 'orders');
    }

    getOrderedDetailed(id: number){
        return this.http.get<Order>(this.baseUrl + 'orders/' + id);
    }
}
