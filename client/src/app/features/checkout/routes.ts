import { Route } from "@angular/router";
import { CheckoutComponent } from "./checkout.component";
import { authGuard } from "../../core/guards/auth-guard";
import { emptyCartGuardGuard } from "../../core/guards/empty-cart-guard-guard";
import { CheckoutSuccessComponent } from "./checkout-success/checkout-success.component";
import { orderCompleteGuard } from "../../core/guards/order-complete-guard";

export const chekcoutRoutes: Route[] = [
        {path:'',component: CheckoutComponent, canActivate:[authGuard, emptyCartGuardGuard]},
        {path:'success',component: CheckoutSuccessComponent, 
            canActivate:[authGuard, orderCompleteGuard]},
    
]