import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';
import { MgrModuleService } from './mgr-module.service';

@Injectable({
  providedIn: 'root'
})
export class StorageInsightsService {
  constructor(private mgrModuleService: MgrModuleService) {}

  getStorageInsightsConfig(): Observable<string> {
    return this.mgrModuleService
      .getConfig('dashboard')
      .pipe(map((config: any) => config.STORAGE_INSIGHTS_REMIND_LATER_ON));
  }

  getStorageInsightsStatus(): Observable<boolean> {
    return this.mgrModuleService.getConfig('call_home_agent').pipe(
      map((config: any) => {
        return config.owner_tenant_id ? true : false;
      })
    );
  }
}
