import { Service, signal } from '@angular/core';

@Service()
export class BusyService {
    loading = signal(false);
    busyRequestCount = 0;

    busy(){
        this.loading.update(value => !value);
    }

    idle(){
        this.loading.update(value => !value);
    }    
}
