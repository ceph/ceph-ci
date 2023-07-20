import { EventEmitter, Injectable, OnDestroy, Output } from '@angular/core';
import { Observable, BehaviorSubject, Subscription, of } from 'rxjs';
import { mergeMap, tap } from 'rxjs/operators';
import { StorageInsightsService } from '../api/storage-insights.service';
import { environment } from '~/environments/environment';

@Injectable({
  providedIn: 'root'
})
export class StorageInsightsNotificationService implements OnDestroy {
  public remindLaterOn$: Observable<boolean>;
  public remindLaterOnSource = new BehaviorSubject<boolean>(false);
  private environment = environment;

  @Output()
  update: EventEmitter<boolean> = new EventEmitter<boolean>();

  private subscription: Subscription;
  constructor(private storageInsightsService: StorageInsightsService) {
    this.remindLaterOn$ = this.remindLaterOnSource.asObservable();
    if (this.environment.build === 'ibm') {
      this.subscription = of(true)
        .pipe(
          mergeMap(() =>
            this.storageInsightsService.getStorageInsightsStatus().pipe(
              mergeMap((status) => {
                if (!status) return this.storageInsightsService.getStorageInsightsConfig();
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
