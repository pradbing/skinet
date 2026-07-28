import { Component, DestroyRef, inject, OnInit, signal } from '@angular/core';
import { ShopService } from '../../../core/services/shop.service';
import { ActivatedRoute } from '@angular/router';
import { Product } from '../../../shared/models/product';
import { filter, map, tap } from 'rxjs';
import { createSignal } from '@angular/core/primitives/signals';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { httpResource } from '@angular/common/http';
import { getCurrentInjector } from '@angular/core/primitives/di';
import { CurrencyPipe } from '@angular/common';
import { MatButton } from '@angular/material/button';
import { MatIcon } from '@angular/material/icon';
import { MatFormField, MatLabel } from '@angular/material/select';
import { MatInput } from '@angular/material/input';
import { MatDivider } from '@angular/material/divider';


@Component({
  selector: 'app-product-details',
  standalone:true,
  imports: [
    CurrencyPipe,
    MatButton,
    MatIcon,
    MatFormField,
    MatInput,
    MatLabel,
    MatDivider
  ],
  templateUrl: './product-details.component.html',
  styleUrl: './product-details.component.scss',
})
export class ProductDetailsComponent  implements OnInit{
  private shopService = inject(ShopService);
  private activatedRoute = inject(ActivatedRoute);

  private readonly destroyRef = inject(DestroyRef);
  product = signal<Product | undefined>(undefined) ;

  ngOnInit(): void {
    this.loadProduct();
  }

  loadProduct(){
    const id=this.activatedRoute.snapshot.paramMap.get('id');
    if(!id) return;

  // this.shopService.getProduct(+id).pipe(
  //     tap(data => this.product?.set(data)),
  //       takeUntilDestroyed(this.destroyRef)
  //   ).subscribe();    
    this.shopService.getProduct(+id).subscribe({
      next:product => {
        this.product?.set(product)
      },
      error: error => console.log(error)
    }) 
  }
  

}
