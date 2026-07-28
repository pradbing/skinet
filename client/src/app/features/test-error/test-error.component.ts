import { HttpClient } from '@angular/common/http';
import { Component, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { MatButton } from '@angular/material/button';
import { catchError, tap, throwError } from 'rxjs';

@Component({
  selector: 'app-test-error',
  imports: [
    MatButton
  ],
  templateUrl: './test-error.component.html',
  styleUrl: './test-error.component.scss',
})
export class TestErrorComponent {
  baseUrl = "https://localhost:5001/api/"
  private http = inject(HttpClient);
  validationErrors = signal<string[] | undefined>(undefined);
  get404Error(){
  //   this.http.get(this.baseUrl + 'buggy/notfound').pipe(
  // tap(response => console.log(response)),
  //   catchError((error) => {
  //     console.error(error)
  //     return throwError(()=>error);
  //   })
  // ).subscribe();
  this.http.get(this.baseUrl + 'buggy/notfound').subscribe({
    next: response=> console.log(response),
    error: error=> console.log(error)
  });

  }

  get400Error(){
  //   this.http.get(this.baseUrl + 'buggy/badrequest').pipe(
  // tap(response => console.log(response)),
  //   catchError((error) => {
  //     console.error(error)
  //     return throwError(()=>error);
  //   })
  // ).subscribe();
  
  this.http.get(this.baseUrl + 'buggy/badrequest').subscribe({
    next: response=> console.log(response),
    error: error=> console.log(error)
  });
  }

  get401Error(){
  //   this.http.get(this.baseUrl + 'buggy/unauthorized').pipe(
  // tap(response => console.log(response)),
  //   catchError((error) => {
  //     console.error(error)
  //     return throwError(()=>error);
  //   })
  // ).subscribe();
    this.http.get(this.baseUrl + 'buggy/unauthorized').subscribe({
    next: response=> console.log(response),
    error: error=> console.log(error)
  });
  }

  get500Error(){
  //   this.http.get(this.baseUrl + 'buggy/internalerror').pipe(
  // tap(response => console.log(response)),
  //   catchError((error) => {
  //     console.error(error)
  //     return throwError(()=>error);
  //   })
  //).subscribe();
    this.http.get(this.baseUrl + 'buggy/internalerror').subscribe({
    next: response=> console.log(response),
    error: error=> console.log(error)
  });
  }

  get400ValidationError(){
  //   this.http.post(this.baseUrl + 'buggy/validationerror',{}).pipe(
  // tap(response => console.log(response)),
  //   catchError((error) => {
  //     //console.error(this.validationErrors)
  //     return throwError(()=>this.validationErrors);
  //   })
  // ).subscribe();
    this.http.post(this.baseUrl + 'buggy/validationerror',{}).subscribe({
    next: response=> console.log(response),
    error: error=>this.validationErrors.set(error)
  });
  }
}
