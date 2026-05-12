import { Injectable } from '@angular/core';
import { Observable, of } from 'rxjs';
import { map } from 'rxjs/operators';
import { MgrModuleService } from './mgr-module.service';
import { AuthStorageService } from '../services/auth-storage.service';
import { Permission } from '../models/permissions';

@Injectable({
  providedIn: 'root'
})
export class StorageInsightsService {
  permission: Permission;
  constructor(
    private mgrModuleService: MgrModuleService,
    private authStorageService: AuthStorageService
  ) {
    this.permission = this.authStorageService.getPermissions()?.configOpt;
  }

  getStorageInsightsConfig(): Observable<string> {
    return this.permission?.read
      ? this.mgrModuleService
          .getConfig('dashboard')
          .pipe(map((config: any) => config.STORAGE_INSIGHTS_REMIND_LATER_ON))
      : of(null);
  }

  getStorageInsightsStatus(): Observable<boolean> {
    return this.permission.read
      ? this.mgrModuleService.getConfig('call_home_agent').pipe(
          map((config: any) => {
            return config.owner_tenant_id ? true : false;
          })
        )
      : of(false);
  }
}
