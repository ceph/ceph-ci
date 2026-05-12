import { Injectable } from '@angular/core';
import { Observable, of } from 'rxjs';
import { map } from 'rxjs/operators';
import { MgrModuleService } from './mgr-module.service';
import { HttpClient } from '@angular/common/http';
import { ConnectivityStatus } from '../models/call-home.model';
import { AuthStorageService } from '../services/auth-storage.service';
import { Permission } from '../models/permissions';

@Injectable({
  providedIn: 'root'
})
export class CallHomeService {
  baseURL = 'api/call_home';
  permissions: Permission;

  constructor(
    private http: HttpClient,
    private mgrModuleService: MgrModuleService,
    private authStorageService: AuthStorageService
  ) {
    this.permissions = this.authStorageService.getPermissions()?.configOpt;
  }

  list(params: any) {
    return this.http.get<any>(`${this.baseURL}/${params.ibmId}/
    ${params.companyName}/${params.firstName}/${params.lastName}/${params.email}`);
  }

  set(params: any) {
    return this.http.put<any>(
      `${this.baseURL}/${params.tenantId}/${params.ibmId}/${params.companyName}/
    ${params.firstName}/${params.lastName}/${params.email}`,
      {}
    );
  }

  getCallHomeConfig(): Observable<string> {
    if (this.permissions?.read) {
      return this.mgrModuleService
        .getConfig('dashboard')
        .pipe(map((config: any) => config.CALL_HOME_REMIND_LATER_ON));
    }
    return of(null);
  }

  getCallHomeStatus(): Observable<boolean> {
    if (this.permissions?.read) {
      return this.mgrModuleService.list().pipe(
        map((moduleData: any) => {
          const callHomeModule = moduleData.find(
            (module: any) => module.name === 'call_home_agent'
          );
          return callHomeModule ? callHomeModule.enabled : false;
        })
      );
    }
    return of(false);
  }

  downloadReport(type: string) {
    return this.http.get(`${this.baseURL}/download/${type}`);
  }

  info() {
    return this.http.get(`${this.baseURL}/info`);
  }

  status(): Observable<ConnectivityStatus> {
    return this.http.get<ConnectivityStatus>(`${this.baseURL}/status`);
  }

  testConnectivity() {
    return this.http.post(`${this.baseURL}/connectivity`, {});
  }

  confirmAutoEnabled(): Observable<void> {
    return this.http.post<void>(`${this.baseURL}/confirm_auto_enabled`, {});
  }
}
