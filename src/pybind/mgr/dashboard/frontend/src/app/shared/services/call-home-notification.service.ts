import { EventEmitter, Injectable, OnDestroy, Output } from '@angular/core';
import { Observable, BehaviorSubject, Subscription, of } from 'rxjs';
import { mergeMap, tap } from 'rxjs/operators';
import { CallHomeService } from '../api/call-home.service';

@Injectable({
  providedIn: 'root'
})
export class CallHomeNotificationService implements OnDestroy {
  public remindLaterOn$: Observable<boolean>;
  public remindLaterOnSource = new BehaviorSubject<boolean>(false);

  @Output()
  update: EventEmitter<boolean> = new EventEmitter<boolean>();

  private subscription: Subscription;
  constructor(private callHomeService: CallHomeService) {
    this.remindLaterOn$ = this.remindLaterOnSource.asObservable();

    this.subscription = of(true)
      .pipe(
        mergeMap(() =>
          this.callHomeService.getCallHomeStatus().pipe(
            mergeMap((status) => {
              if (!status) return this.callHomeService.getCallHomeConfig();
              else return of('never');
            }),
            tap((remindLaterOn: string) => {
              const dateNow = new Date().toDateString();
              const visible =
                remindLaterOn === dateNow || remindLaterOn === '' || remindLaterOn === null;
              this.remindLaterOnSource.next(visible);
              this.update.emit(visible);
            })
          )
        )
      )
      .subscribe();
  }

  hide() {
    this.remindLaterOnSource.next(false);
    this.update.emit(false);
  }

  ngOnDestroy(): void {
    this.subscription.unsubscribe();
  }

  setVisibility(visible: boolean) {
    this.remindLaterOnSource.next(visible);
    this.update.emit(visible);
  }
}
